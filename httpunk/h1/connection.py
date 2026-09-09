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
# the body follows as its own write. Measured (BENCH_FINDINGS §3): the one-write
# path wins at every size up to 100 KB. Streamed bodies are never coalesced: the
# head must never wait on the app's generator (hyper polls the body after the head).


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
        immediate `bytes` body up to the codec's `max_buf_size` is COALESCED with
        the head into a single transport write — hyper's `WriteBuf` Flatten strategy
        (proto/h1/io.rs), see the module comment. Streamed bodies, and immediate ones
        past the cap, keep the head-first write. The coalesced branch mirrors
        `_send_body` exactly (aiter_body yields a bytes body as one chunk)."""
        if body is None or (isinstance(body, (bytes, bytearray)) and len(body) <= codec.max_buf_size):
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
        # `serialize_end` is always CALLED (it is what raises on a body short of its
        # Content-Length) but only WRITTEN when it has bytes: the chunked terminator.
        # A content-length body ends with nothing on the wire (hyper `end_body`).
        if codec.body_is_eof():
            end = codec.serialize_end()
            if end:
                await self.write(end)
            return
        if body is not None:
            async for chunk in aiter_body(body):
                chunk = codec.serialize_data(bytes(chunk))
                if chunk:
                    await self.write(chunk)
        end = codec.serialize_trailers(trailers) if trailers is not None else codec.serialize_end()
        if end:
            await self.write(end)
