"""HTTP/1 connection driver base — the role-agnostic leaf layer shared by the
client `Connection` (client.py) and the server `ServerConnection` (server.py),
mirroring hyper's generic `Conn<T: Http1Transaction>` (the connection state +
IO shared by both roles).

HTTP/1 is a role *inversion* — the client writes a request then reads a response;
the server reads a request then writes a response — so the orchestration is
disjoint and lives in the role subclasses (their respective `client.py`/`server.py`,
like h2). Only the transport-ownership + body-framing/send + teardown leaves are
genuinely shared, and they live here. All byte work is the Rust sans-IO core
(`H1Codec` head parse/encode + body encode, `H1BodyDecoder` body decode).

Cross-reference: hyperium/hyper 1.11.1 `src/proto/h1/{conn,dispatch,role}.rs`.
"""

from .. import _backend
from .._common import aiter_body
from ..exceptions import ConnectionClosedError


_READ_SIZE = 65536

# Coalescing cutoff for `_send_head_and_body`: an immediate bytes body at or
# under this size is copied into the head's buffer and written in ONE syscall.
# Small enough that the memcpy is far cheaper than the syscall it saves; large
# bodies don't need it (bulk writes of full segments don't Nagle-stall) and
# copying them would just churn memory. The VALUE is ours, not hyper's: hyper
# needs no cutoff — its WriteBuf either flattens bodies of any size (bounded by
# max_buf_size, ~417KB default) or queues chunks copy-free for a vectored
# writev. A copy cap stands in for that writev path, which the transport seam's
# single-buffer `send_all` can't express; a future `send_vectored` on the seam
# would retire the cutoff and match hyper's Queue strategy outright.
_COALESCE_MAX = 8192


class H1ConnectionBase:
    """Role-agnostic h1 connection leaves over a caller-supplied transport
    (hyper's `Conn`). Subclassed by the client `Connection` and the server
    `ServerConnection`, which add the role-specific orchestration."""

    def __init__(self, transport, *, backend=None):
        self.transport = transport
        self.backend = _backend.resolve(backend)
        self._closed = False
        # Set once the connection is handed off as a raw tunnel (101 / CONNECT):
        # the transport belongs to an `H1Upgraded` the caller owns — don't close it.
        self._upgraded = False

    def _close_transport(self):
        # Close + null the transport, unless it was handed to an `H1Upgraded`
        # tunnel (hyper: after `on_upgrade`/`into_parts` the driver no longer owns
        # it). Nulling makes a later close / failure path a no-op.
        if self.transport is not None and not self._upgraded:
            self.backend.close_transport(self.transport)
            self.transport = None

    async def close(self):
        self._closed = True
        self._close_transport()

    def _detach(self):
        """Relinquish the transport to a caller-owned `H1Upgraded` tunnel (101 /
        CONNECT): no more requests, and neither `close` nor a failure path may
        touch the transport again (hyper `Connection::into_parts`)."""
        self._upgraded = True
        self._closed = True
        self.transport = None

    def read_body_more(self):
        """Read more transport bytes for an in-flight body. Empty bytes = EOF."""
        # A concurrent close() may have nulled the transport (its close-first teardown).
        # Capture it once and raise a clean connection error rather than an
        # AttributeError on `None.receive_some` (F59). The local capture also closes the
        # check-then-null race under free-threading.
        transport = self.transport
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.receive_some(_READ_SIZE)

    def write(self, data):
        transport = self.transport
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.send_all(data)

    @staticmethod
    def _body_framing(body):
        # None / empty bytes -> no body framing (hyper `set_length` None branch,
        # role.rs L1311-1316); non-empty bytes -> Content-Length; (async) iterable
        # -> chunked. The request and response framing rules are the same, so this
        # is shared.
        if body is None:
            return None, False
        if isinstance(body, (bytes, bytearray)):
            return (len(body), False) if len(body) else (None, False)
        return None, True

    async def _send_head_and_body(self, codec, head, body, trailers=None):
        """Write the message head + framed body. A bodyless message or an
        immediate small `bytes` body (≤ `_COALESCE_MAX`) is COALESCED with the
        head into a single transport write — hyper's `WriteBuf` "flatten"
        strategy (proto/h1/io.rs): one syscall instead of two, and never two
        small back-to-back segments, so the Nagle × delayed-ACK stall (~40ms
        per message on sockets without TCP_NODELAY) is structurally impossible
        for small messages. Streamed/large bodies keep the head-first write —
        the head must never wait on a body generator, and copying bulk data
        would cost more than the saved syscall. The coalesced branch mirrors
        `_send_body` exactly (aiter_body yields a bytes body as one chunk)."""
        if body is None or (isinstance(body, (bytes, bytearray)) and len(body) <= _COALESCE_MAX):
            buf = bytearray(head)
            if codec.body_is_eof():
                buf += codec.serialize_end()
            else:
                if body is not None:
                    buf += codec.serialize_data(bytes(body))
                buf += codec.serialize_trailers(trailers) if trailers is not None else codec.serialize_end()
            await self.write(bytes(buf))
            return
        await self.write(head)
        await self._send_body(codec, body, trailers)

    async def _send_body(self, codec, body, trailers=None):
        # Frame + write the message body via `codec` (the request codec on the
        # client, the response codec on the server). A bodyless framing —
        # `codec.body_is_eof()` for a HEAD/204/304 response, or a `body is None`
        # length/close framing — writes no body: hyper never polls the body when the
        # encoder is eof (conn.rs write_head), so the iterable is skipped and its side
        # effects don't fire (G37). `trailers` (a HeaderMap, chunked bodies only)
        # terminate the body with a trailer block instead of a bare `0\r\n\r\n` (F45);
        # a request with trailers is always chunked, so it is never `body_is_eof`.
        # All writes go through `write`, which raises a clean ConnectionClosedError once the
        # transport was nulled by a teardown (F59) — a body pump orphaned by an abandoned
        # exchange wakes into that, not into an AttributeError on `None.send_all`.
        if codec.body_is_eof():
            await self.write(codec.serialize_end())
            return
        if body is not None:
            async for chunk in aiter_body(body):
                await self.write(codec.serialize_data(bytes(chunk)))
        if trailers is not None:
            await self.write(codec.serialize_trailers(trailers))
        else:
            await self.write(codec.serialize_end())
