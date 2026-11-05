"""Derived runtime tests: dropping/failing a handshake drops its IO, not a TLS shutdown.
No upstream test mirrors Python context-entry cancellation or the spawned driver tasks.
"""

import pytest
from _net import read_to_eof, tcp_pair
from tonio.colored import Event, scope
from tonio.exceptions import CancelledError

from httpunk import H2Connection
from httpunk.exceptions import ConnectionClosedError
from httpunk.h2.connection import PREFACE
from httpunk.h2.server import H2Server


class _ObservedStream:
    def __init__(self, stream, *, gate_write=False):
        self.stream = stream
        self.read_started = Event()
        self.write_started = Event()
        self.write_gate = Event() if gate_write else None
        self.aborts = 0

    async def receive_some(self, size):
        self.read_started.set()
        return await self.stream.receive_some(size)

    async def send_all(self, data):
        self.write_started.set()
        if self.write_gate is not None:
            await self.write_gate.wait()
            raise OSError("handshake write failed")
        await self.stream.send_all(data)

    def close(self):
        self.aborts += 1
        self.stream.close()

    abort = close


@pytest.mark.tonio
@pytest.mark.parametrize("outcome", ["cancel", "eof"])
async def test_h2_client_handshake_aborts_transport_and_pumps(outcome):
    async with tcp_pair() as (stream, peer):
        observed = _ObservedStream(stream)
        conn = H2Connection(observed)
        finished = Event()
        errors = []

        async def connect():
            try:
                async with conn:
                    pytest.fail("a peer without SETTINGS cannot finish the handshake")
            except BaseException as exc:
                errors.append(exc)
                if not isinstance(exc, Exception):
                    raise
            finally:
                finished.set()

        async with scope() as s:
            s.spawn(connect())
            preface = b""
            while len(preface) < len(PREFACE):
                preface += await peer.receive_some(65536)
            assert preface.startswith(PREFACE)
            await observed.read_started.wait()
            if outcome == "cancel":
                s.cancel()
            else:
                peer.close()
        await finished.wait()
        assert len(errors) == 1
        if outcome == "cancel":
            assert isinstance(errors[0], CancelledError)
            await read_to_eof(peer, allow_reset=True)
        else:
            assert isinstance(errors[0], ConnectionClosedError)
        assert observed.aborts == 1
        assert conn.closed
        error = conn._conn.conn_error()
        assert error is not None
        assert error.__traceback__ is None
        if outcome == "eof":
            assert errors[0] is not error
            assert errors[0].args == error.args
        # Join only: no close(), cancel, or extra signal can hide a leaked pump.
        write = conn._conn.take_pump_handle()
        read = conn._conn.take_read_handle()
        assert write is not None and read is not None
        await write
        await read


@pytest.mark.tonio
@pytest.mark.parametrize("outcome", ["cancel", "error"])
async def test_h2_server_handshake_aborts_during_initial_flush(outcome):
    async with tcp_pair() as (peer, stream):
        observed = _ObservedStream(stream, gate_write=True)
        server = H2Server(observed)
        finished = Event()
        errors = []

        async def start():
            try:
                async with server:
                    pytest.fail("the initial flush is blocked")
            except BaseException as exc:
                errors.append(exc)
                if not isinstance(exc, Exception):
                    raise
            finally:
                finished.set()

        async with scope() as s:
            s.spawn(start())
            await observed.write_started.wait()
            if outcome == "cancel":
                s.cancel()
            else:
                observed.write_gate.set()
        await finished.wait()
        assert len(errors) == 1
        if outcome == "cancel":
            assert isinstance(errors[0], CancelledError)
        else:
            assert isinstance(errors[0], OSError)
            assert errors[0].args == ("handshake write failed",)
        assert observed.aborts == 1
        assert server._conn.is_closed()
        assert server._conn.take_pump_handle() is None
        assert server._conn.take_read_handle() is None
        await read_to_eof(peer, allow_reset=True)
