"""The stored-error hygiene rules (`exceptions.fresh_exc`): saved connection/
stream errors are traceback-free copies, and every raise of a saved error is a
fresh copy chained to it — so no stored instance ever accumulates a consumer
stack's frames into a refcount-invisible cycle (the class of bug where a failed
connection's error pinned live transports until a gen-2 cyclic GC)."""

import gc
import platform
import weakref

import pytest

from httpunk._backend.asyncio import AsyncioBackend, _AsyncioStream
from httpunk.exceptions import GoAwayError, H2Reason, StreamResetError, fresh_exc
from httpunk.h1.client import Connection as H1Driver


# ----- fresh_exc -----


def _caught(exc):
    """Raise + catch `exc` so it carries a traceback, like any stored real error."""
    try:
        raise exc
    except type(exc) as e:
        return e


def test_fresh_exc_copies_oserror_family():
    err = _caught(ConnectionResetError(54, "Connection reset by peer"))
    cp = fresh_exc(err)
    assert cp is not err
    assert type(cp) is ConnectionResetError
    assert cp.args == err.args
    assert (cp.errno, cp.strerror) == (54, "Connection reset by peer")
    assert cp.__traceback__ is None  # the copy starts frame-free
    assert err.__traceback__ is not None  # the original is untouched


def test_fresh_exc_carries_dict_extras():
    err = ValueError("poisoned")
    err.request_unsent = True
    cp = fresh_exc(err)
    assert cp is not err
    assert cp.request_unsent is True


def test_fresh_exc_goaway_roundtrip():
    # GoAwayError's args hold the formatted message, so copying needs its
    # __reduce__ — with both a known (enum) and an unknown (raw int) error code.
    for code in (int(H2Reason.ENHANCE_YOUR_CALM), 0x5EAF00D):
        err = _caught(GoAwayError(7, code, b"bye"))
        cp = fresh_exc(err)
        assert cp is not err
        assert type(cp) is GoAwayError
        assert (cp.last_stream_id, cp.error_code, cp.debug_data) == (err.last_stream_id, err.error_code, b"bye")
        assert str(cp) == str(err)
        assert cp.__traceback__ is None


def test_fresh_exc_stream_reset_roundtrip():
    err = _caught(StreamResetError(5, int(H2Reason.CANCEL)))
    cp = fresh_exc(err)
    assert cp is not err
    assert (cp.stream_id, cp.error_code) == (5, H2Reason.CANCEL)
    assert cp.__traceback__ is None


def test_fresh_exc_degrades_to_shared_instance():
    class OddError(Exception):
        def __init__(self, a, b):  # args don't roundtrip (message-only super().__init__)
            super().__init__(f"{a}-{b}")

    err = OddError(1, 2)
    assert fresh_exc(err) is err  # uncopyable -> degraded but functional


# ----- the asyncio backend stream -----


@pytest.mark.asyncio
async def test_connection_lost_strips_stored_traceback():
    s = _AsyncioStream()
    s.connection_made(None)
    s.connection_lost(_caught(ConnectionResetError(54, "peer reset")))
    assert s._error.__traceback__ is None


@pytest.mark.asyncio
async def test_receive_some_raises_a_fresh_copy():
    s = _AsyncioStream()
    s.connection_made(None)
    s.connection_lost(ConnectionResetError(54, "peer reset"))
    with pytest.raises(ConnectionResetError) as ei:
        await s.receive_some()
    assert ei.value is not s._error  # never the stored instance
    assert ei.value.__cause__ is s._error  # chained for debuggability
    assert ei.value.args == s._error.args
    assert s._error.__traceback__ is None  # the raise left no frames on the stored error


@pytest.mark.asyncio
async def test_send_all_raises_a_fresh_copy():
    s = _AsyncioStream()
    s.connection_made(None)
    s.connection_lost(ConnectionResetError(54, "peer reset"))
    with pytest.raises(ConnectionResetError) as ei:
        await s.send_all(b"data")
    assert ei.value is not s._error
    assert s._error.__traceback__ is None


class _Sentinel:
    """Stands in for everything a consumer frame holds (a server's response
    transport, a relay generator, ...)."""


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="asserts prompt refcount finalization — CPython semantics; on PyPy everything waits for GC regardless",
)
@pytest.mark.asyncio
async def test_failed_stream_frees_consumer_state_without_cyclic_gc():
    # The end-to-end property (the "s04" regression): after a consumer stack
    # catches the read error of a failed connection, everything — the consumer's
    # state AND the stream itself — must be freed by refcounting alone. Before
    # the fresh_exc rules, the stored error's traceback closed a cycle through
    # the stream and pinned the whole graph until a gen-2 collection.
    s = _AsyncioStream()
    s.connection_made(None)
    s.connection_lost(ConnectionResetError(54, "peer reset"))
    stream_ref = weakref.ref(s)
    sentinel = _Sentinel()
    sentinel_ref = weakref.ref(sentinel)

    async def relay(stream, held):
        try:
            while True:
                await stream.receive_some()
        except ConnectionResetError:
            pass  # swallowed, like a server relaying this body would

    gc.disable()
    try:
        await relay(s, sentinel)
        del sentinel
        assert sentinel_ref() is None  # consumer state: freed by refcount
        del s
        assert stream_ref() is None  # the stream: no self-pinning cycle either
    finally:
        gc.enable()


# ----- the h1 driver's saved connection error -----


class _DummyTransport:
    def close(self):
        pass

    def abort(self):  # the asyncio seam's abortive close (`_AsyncioStream.abort`)
        pass


@pytest.mark.asyncio
async def test_h1_fail_stores_a_stripped_copy():
    # `_fail` runs while the caught instance is still propagating to the caller
    # (send_request stores, then re-raises) — the saved error must be a copy so
    # the continuing propagation can't grow a stored traceback.
    conn = H1Driver(_DummyTransport(), backend=AsyncioBackend())
    err = _caught(ValueError("mid-exchange failure"))
    conn._fail(err)
    assert conn.error is not err
    assert conn.error.args == err.args
    assert conn.error.__traceback__ is None


@pytest.mark.asyncio
async def test_h1_wait_idle_raises_a_fresh_copy():
    conn = H1Driver(_DummyTransport(), backend=AsyncioBackend())
    conn._fail(_caught(ValueError("dead")))
    with pytest.raises(ValueError) as ei:
        await conn.wait_idle()
    assert ei.value is not conn.error
    assert ei.value.__cause__ is conn.error
    assert conn.error.__traceback__ is None  # this raise added nothing to the stored error
