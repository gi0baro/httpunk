"""Test-side stand-in for the piece of tonio's `SocketStream` the backend's bounded
reader binds at connection setup (`TonioBackend.bounded_reader`): the no-wait read
`receive_some_nowait(n)` -> bytes | b"" | `NotReady`, and `waiter_readable(micros)` ->
None (readable now) or a waiter to park on. A stub transport mixes this in and plugs its
buffer in through three sync hooks: `_readable()`, `_recv_now(n)` (raises
`BlockingIOError` when nothing is there), and `_park(timeout_micros)` -> an awaitable
that resumes when bytes/EOF may have landed or the timer fired (never called by a stub
that is always readable)."""


class StubStream:
    class NotReady:  # the stream's own sentinel, compared by identity (`transport.NotReady`)
        pass

    def receive_some_nowait(self, max_bytes=65536):
        try:
            return self._recv_now(max_bytes)
        except BlockingIOError:
            return self.NotReady

    def waiter_readable(self, timeout=None):
        if self._readable():
            return None
        return self._park(timeout)
