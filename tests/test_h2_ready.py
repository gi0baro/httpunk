"""`H2Connection.ready()` / `StreamManager.wait_until_ready` — verify it really
*awaits* for a MAX_CONCURRENT_STREAMS slot (h2 `SendRequest::ready`), rather than
returning synchronously. These are socket-free unit tests against the mechanism."""

import pytest
from tonio.colored import Event, scope, sleep

from httpunk import GoAwayError, H2Reason
from httpunk.exceptions import ConnectionClosedError
from httpunk.h2.client import Connection


@pytest.mark.tonio
async def test_wait_until_ready_blocks_until_permit_free():
    conn = Connection(None)  # constructed only; never connected
    mgr = conn.streams
    mgr._apply_stream_limit(1)  # a single MAX_CONCURRENT_STREAMS slot
    await mgr._acquire_stream_slot()  # occupy it (increments the open-stream count)

    returned = Event()

    async with scope() as s:

        async def probe():
            await mgr.wait_until_ready()  # must suspend: the only slot is taken
            returned.set()

        s.spawn(probe())
        await sleep(0.03)
        blocked_while_full = not returned.is_set()
        mgr._release_count()  # free the slot (decrements the count, wakes waiters)
        await returned.wait()  # ready now resolves
        s.cancel()

    assert blocked_while_full


@pytest.mark.tonio
async def test_ready_returns_when_unlimited_and_raises_after_goaway():
    conn = Connection(None)
    await conn.streams.wait_until_ready()  # no negotiated limit -> ready at once

    conn.streams._goaway = GoAwayError(0, H2Reason.NO_ERROR, b"")
    with pytest.raises(GoAwayError):
        await conn.streams.wait_until_ready()


@pytest.mark.tonio
async def test_ready_prefers_goaway_over_eof_error():
    """After a graceful GOAWAY the peer closes, so both `_goaway` (GoAwayError) and the
    EOF `_conn.error` (ConnectionClosedError) end up set. `ready()` must surface the
    retry-relevant GoAwayError, not the EOF error (F20)."""
    conn = Connection(None)
    conn.streams._goaway = GoAwayError(0, H2Reason.NO_ERROR, b"")
    conn.error = ConnectionClosedError("connection closed by peer")  # EOF after the GOAWAY
    with pytest.raises(GoAwayError):
        await conn.streams.wait_until_ready()


@pytest.mark.tonio
async def test_acquire_slot_fails_promptly_on_goaway():
    """A request parked at MAX_CONCURRENT_STREAMS when a GOAWAY arrives fails at once
    (GoAwayError), not after waiting for a surviving stream to free a slot (F20)."""
    conn = Connection(None)
    mgr = conn.streams
    mgr._apply_stream_limit(1)
    await mgr._acquire_stream_slot()  # occupy the only slot

    raised = Event()

    async with scope() as s:

        async def probe():
            try:
                await mgr._acquire_stream_slot()  # parked: no free slot
            except GoAwayError:
                raised.set()

        s.spawn(probe())
        await sleep(0.03)
        assert not raised.is_set()  # still parked — no GOAWAY yet, and no slot freed
        mgr._goaway = GoAwayError(0, H2Reason.NO_ERROR, b"")
        mgr._on_go_away(0, mgr._goaway)  # wakes the slot waiter to re-check
        await raised.wait()  # fails promptly, without any slot ever freeing
        s.cancel()
