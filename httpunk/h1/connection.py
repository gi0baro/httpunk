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

Cross-reference: hyperium/hyper 1.13.0 `src/proto/h1/{conn,dispatch,role}.rs`.
"""

from .._common import aiter_body, body_write_error, connection_closed_error, h1_body_error


_READ_SIZE = 65536

# Coalescing: an immediate bytes body is written together with the head, in ONE
# buffer and one syscall, up to the codec's `max_buf_size` (hyper's, 417 KB by
# default). hyper's `WriteBuf` has two strategies: `Queue` over a vectored IO (head
# and body go out in one writev, nothing copied) and `Flatten` otherwise (the body
# is memcpy'd after the head, whatever its size). The transport seam is a
# single-buffer `send_all`, so only Flatten is reachable here; the copy is one
# exact-size write (`serialize_head_and_body`). The cap is where the divergence
# lives: hyper's Flatten has none, and `max_buf_size` is hyper's own bound on how
# much it buffers before flushing — past it the memcpy is worth about the syscall
# it saves and a huge body would be doubled in memory, so the head goes first and
# the body follows as its own write. Measured: the one-write
# path wins at every size up to 100 KB. Streamed bodies are never coalesced: the
# head must never wait on the app's generator (hyper polls the body after the head).


# The body's shape (`types.Body`), classified once by `_body_plan`: how it is written.
BODY_NONE = 0  # no body
BODY_BYTES = 1  # an immediate `bytes` / `bytearray`
BODY_SYNC = 2  # a sync iterable of chunks: written inline
BODY_ASYNC = 3  # an async iterable: pumped in its own task (may park between chunks)


class H1Framing:
    """The body-framing + send leaves shared by both roles (hyper's `Conn` write
    half: `encode_head`'s body length, `write_body`/`end_body`). Pure orchestration
    over `self.write` — no state of its own.

    A write raises `BodyWriteError` and a body read `H1BodyError` (hyper
    dispatch.rs:468-473 `new_body_write`, :307-309 `new_body`), from what the transport
    raised — or, once the connection was closed locally, from that close, whatever
    the transport did then (`EOF` on asyncio, `EBADF` on tonio)."""

    def _write_error(self, exc):
        """The `BodyWriteError` for a write that raised `exc`, its `__cause__` set."""
        cause = connection_closed_error() if self.closed_locally else exc
        err = body_write_error(cause)
        err.__cause__ = cause
        return err

    def _raise_write_failed(self, exc):
        # Called from the `except` at the write, while `exc` is live.
        raise self._write_error(exc)

    def _raise_read_failed(self, exc):
        """Raise the `H1BodyError` for a body read that raised `exc` (called from the
        `except` at the read, while `exc` is live), or that the local close ended."""
        cause = connection_closed_error() if self.closed_locally else exc
        raise h1_body_error(cause) from cause

    @staticmethod
    def _body_plan(body, max_buf_size):
        """The body's shape, classified ONCE for the whole write path — `(content_length,
        chunked, kind, coalesce)`. The framing inputs are hyper's `set_length`: None /
        empty bytes -> no body (role.rs L1353-1359), non-empty bytes -> Content-Length,
        an iterable -> chunked; the request and response rules are the same, so this is
        shared. `kind` is the write path (`BODY_*`: an async iterable must be pumped in
        its own task, a sync one is written inline) and `coalesce` the head+body write
        decision: an immediate body up to `max_buf_size` rides the head's write (see the
        module comment for the rule and its cap). Nothing downstream re-derives any of
        this from `body`."""
        if body is None:
            return None, False, BODY_NONE, True
        if isinstance(body, (bytes, bytearray)):
            n = len(body)
            return n or None, False, BODY_BYTES, n <= max_buf_size
        if hasattr(body, "__aiter__"):
            return None, True, BODY_ASYNC, False
        return None, True, BODY_SYNC, False

    async def _send_head_and_body(self, codec, head, body, trailers, transport, coalesce):
        """Write the message head + framed body. `coalesce` (decided by `_body_plan`: a
        bodyless message or an immediate body up to the codec's `max_buf_size`) writes
        head and body as ONE transport write — hyper's `WriteBuf` Flatten strategy
        (proto/h1/io.rs), see the module comment; the coalesced branch mirrors
        `_send_body` exactly (aiter_body yields a bytes body as one chunk). Otherwise —
        a streamed body, or an immediate one past the cap — the head goes first. The
        head's write goes to `transport`, the one the caller's head step took from the
        state (`None` = closed by then: the failure a write into it would raise); the
        body's chunks go through `write`, each fetching the transport anew."""
        if transport is None:
            self._raise_write_failed(connection_closed_error())
        if coalesce:
            data = codec.serialize_head_and_body(head, body, trailers)
            try:
                await transport.send_all(data)
            except Exception as exc:
                self._raise_write_failed(exc)
            return
        try:
            await transport.send_all(head)
        except Exception as exc:
            self._raise_write_failed(exc)
        await self._send_body(codec, body, trailers)

    async def _send_body(self, codec, body, trailers=None):
        # Frame + write the message body via `codec` (the request codec on the
        # client, the response codec on the server). A bodyless framing —
        # `codec.body_is_eof()` for a HEAD/204/304 response, or a `body is None`
        # length/close framing — writes no body: hyper never polls the body when the
        # encoder is eof (conn.rs write_head), so the iterable is skipped and its side
        # effects don't fire. `trailers` (a HeaderMap, chunked bodies only)
        # terminate the body with a trailer block instead of a bare `0\r\n\r\n`;
        # a request with trailers is always chunked, so it is never `body_is_eof`.
        # All writes go through `write`, which raises a clean ConnectionClosedError once the
        # state gave the transport away — a body pump orphaned by an abandoned
        # exchange wakes into that, not into an AttributeError on `None.send_all`. Only
        # the writes are wrapped: the body's iteration and the codec's framing raise as
        # themselves.
        # `serialize_end` is always CALLED (it is what raises on a body short of its
        # Content-Length) but only WRITTEN when it has bytes: the chunked terminator.
        # A content-length body ends with nothing on the wire (hyper `end_body`).
        if codec.body_is_eof():
            end = codec.serialize_end()
            if end:
                try:
                    await self.write(end)
                except Exception as exc:
                    self._raise_write_failed(exc)
            return
        if body is not None:
            async for chunk in aiter_body(body):
                chunk = codec.serialize_data(bytes(chunk))
                if chunk:
                    try:
                        await self.write(chunk)
                    except Exception as exc:
                        self._raise_write_failed(exc)
        end = codec.serialize_trailers(trailers) if trailers is not None else codec.serialize_end()
        if end:
            try:
                await self.write(end)
            except Exception as exc:
                self._raise_write_failed(exc)
