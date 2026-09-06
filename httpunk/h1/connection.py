"""HTTP/1 framing leaves — the role-agnostic layer shared by the client
`Connection` (client.py) and the server `ServerConnection` (server.py): the
write half of hyper's generic `Conn<T: Http1Transaction>` (body framing + send).
The connection STATE of each role is Rust (`H1ClientState` / `H1ServerState`,
src/h1/conn.rs); each driver subclasses its state and mixes this in.

HTTP/1 is a role *inversion* — the client writes a request then reads a response;
the server reads a request then writes a response — so the orchestration is
disjoint and lives in the role subclasses (their respective `client.py`/`server.py`,
like h2). Only the transport-ownership + body-framing/send + teardown leaves are
genuinely shared, and they live here. All byte work is the Rust sans-IO core
(`H1Codec` head parse/encode + body encode, `H1BodyDecoder` body decode).

Cross-reference: hyperium/hyper 1.11.1 `src/proto/h1/{conn,dispatch,role}.rs`.
"""

from .._common import aiter_body


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


class H1Framing:
    """The body-framing + send leaves shared by both roles (hyper's `Conn` write
    half: `encode_head`'s body length, `write_body`/`end_body`). Pure orchestration
    over `self.write` — no state of its own."""

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
            await self.write(codec.serialize_head_and_body(head, body, trailers))
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
        # state gave the transport away (F59) — a body pump orphaned by an abandoned
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
