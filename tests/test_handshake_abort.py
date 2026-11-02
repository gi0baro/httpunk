"""Derived runtime tests: dropping/failing a handshake drops its IO, not a TLS shutdown.
No upstream test mirrors Python context-entry cancellation or the spawned driver tasks.
"""

import pytest
from _runtime import cancelled_type, connected_pair
from conftest import TONIO_OK

from httpunk import H2Connection
from httpunk._backend import Backend
from httpunk.exceptions import ConnectionClosedError
from httpunk.h2.connection import PREFACE
from httpunk.h2.server import H2Server


class _ObservedStream:
    def __init__(self, stream, backend, *, gate_write=False):
        self.stream = stream
        self.backend = backend
        self.read_started = backend.event()
        self.write_started = backend.event()
        self.write_gate = backend.event() if gate_write else None
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
        self.backend.close_transport(self.stream)

    abort = close


async def _assert_eof(peer, backend):
    try:
        while await peer.receive_some(65536):
            pass
    except backend.broken_transport_errors:
        pass  # an abort with unread bytes may be a reset rather than FIN


async def _client_handshake_aborts_transport_and_pumps(runtime, outcome):
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (stream, peer):
        observed = _ObservedStream(stream, backend)
        conn = H2Connection(observed, backend=backend)
        finished = backend.event()
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

        async with backend.scope() as scope:
            scope.spawn(connect())
            preface = b""
            while len(preface) < len(PREFACE):
                preface += await peer.receive_some(65536)
            assert preface.startswith(PREFACE)
            await observed.read_started.wait()
            if outcome == "cancel":
                scope.cancel()
            else:
                backend.close_transport(peer)
        await finished.wait()
        assert len(errors) == 1
        if outcome == "cancel":
            assert isinstance(errors[0], cancelled_type(runtime))
            await _assert_eof(peer, backend)
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


async def _server_handshake_aborts_during_initial_flush(runtime, outcome):
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (peer, stream):
        observed = _ObservedStream(stream, backend, gate_write=True)
        server = H2Server(observed, backend=backend)
        finished = backend.event()
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

        async with backend.scope() as scope:
            scope.spawn(start())
            await observed.write_started.wait()
            if outcome == "cancel":
                scope.cancel()
            else:
                observed.write_gate.set()
        await finished.wait()
        assert len(errors) == 1
        if outcome == "cancel":
            assert isinstance(errors[0], cancelled_type(runtime))
        else:
            assert isinstance(errors[0], OSError)
            assert errors[0].args == ("handshake write failed",)
        assert observed.aborts == 1
        assert server._conn.is_closed()
        assert server._conn.take_pump_handle() is None
        assert server._conn.take_read_handle() is None
        await _assert_eof(peer, backend)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
@pytest.mark.parametrize("outcome", ["cancel", "eof"])
async def test_h2_client_handshake_aborts_transport_and_pumps_tonio(outcome):
    await _client_handshake_aborts_transport_and_pumps("tonio", outcome)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["cancel", "eof"])
async def test_h2_client_handshake_aborts_transport_and_pumps_asyncio(outcome):
    await _client_handshake_aborts_transport_and_pumps("asyncio", outcome)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
@pytest.mark.parametrize("outcome", ["cancel", "error"])
async def test_h2_server_handshake_aborts_during_initial_flush_tonio(outcome):
    await _server_handshake_aborts_during_initial_flush("tonio", outcome)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["cancel", "error"])
async def test_h2_server_handshake_aborts_during_initial_flush_asyncio(outcome):
    await _server_handshake_aborts_during_initial_flush("asyncio", outcome)
