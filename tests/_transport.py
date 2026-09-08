"""Test-side stand-in for the piece of tonio's `_Socket` the backend's bounded reader
binds at connection setup (`TonioBackend.bounded_reader` -> `transport.socket`):
`_io_arm_r(timeout)` -> None (readable now) or a waiter to park on, `_io_clear_r()`,
and the raw `_sock.recv(n)`. A stub transport plugs its buffer in through three sync
hooks: `_readable()`, `_recv_now(n)` (raises `BlockingIOError` when nothing is there),
and `_park(timeout_micros)` -> an awaitable that resumes when bytes/EOF may have landed
or the timer fired (never called by a stub that is always readable)."""


class StubSocket:
    def __init__(self, stub):
        self._stub = stub
        self._sock = self

    def _io_arm_r(self, timeout=None):
        if self._stub._readable():
            return None
        return self._stub._park(timeout)

    def _io_clear_r(self):
        pass

    def recv(self, n):
        return self._stub._recv_now(n)
