"""`H2Connection.ready()` / the client's `_wait_until_ready` — verify it really
*awaits* for a MAX_CONCURRENT_STREAMS slot (h2 `SendRequest::ready`), rather than
returning synchronously. Socket-free unit tests against the mechanism: the slot
count lives in the Rust state (`try_claim_slot` / `release_slot_count`); GOAWAY and
failure are fed through the same calls the read pump makes."""

import pytest
from tonio.colored import Event, scope, sleep

from httpunk import GoAwayError, H2Reason
from httpunk._httpunk import H2Codec
from httpunk.exceptions import ConnectionClosedError
from httpunk.h2.client import Connection


def _goaway_frame(last_stream_id=0, reason=int(H2Reason.NO_ERROR)):
    [frame] = H2Codec("client").receive(H2Codec("server").serialize_go_away(last_stream_id, reason))
    return frame


@pytest.mark.tonio
async def test_wait_until_ready_blocks_until_permit_free():
    conn = Connection(None)  # constructed only; never connected
    conn.apply_stream_limit(1)  # a single MAX_CONCURRENT_STREAMS slot
    await conn._acquire_slot()  # occupy it (increments the open-stream count)

    returned = Event()

    async with scope() as s:

        async def probe():
            await conn._wait_until_ready()  # must suspend: the only slot is taken
            returned.set()

        s.spawn(probe())
        await sleep(0.03)
        blocked_while_full = not returned.is_set()
        conn.release_slot_count()  # free the slot (decrements the count)...
        conn._slot_evt.set()  # ...and wake the waiters, as a stream close does (FLAG_SLOT_FREED)
        await returned.wait()  # ready now resolves
        s.cancel()

    assert blocked_while_full


@pytest.mark.tonio
async def test_ready_returns_when_unlimited_and_raises_after_goaway():
    conn = Connection(None)
    await conn._wait_until_ready()  # no negotiated limit -> ready at once

    conn._dispatch(_goaway_frame(), None)  # the peer's GOAWAY, as the read pump feeds it
    with pytest.raises(GoAwayError):
        await conn._wait_until_ready()


@pytest.mark.tonio
async def test_ready_prefers_goaway_over_eof_error():
    """After a graceful GOAWAY the peer closes, so both the GOAWAY and the EOF error
    (ConnectionClosedError) end up recorded. `ready()` must surface the retry-relevant
    GoAwayError, not the EOF error (F20)."""
    conn = Connection(None)
    conn._dispatch(_goaway_frame(), None)
    conn._fail(ConnectionClosedError("connection closed by peer"))  # EOF after the GOAWAY
    with pytest.raises(GoAwayError):
        await conn._wait_until_ready()


@pytest.mark.tonio
async def test_acquire_slot_fails_promptly_on_goaway():
    """A request parked at MAX_CONCURRENT_STREAMS when a GOAWAY arrives fails at once
    (GoAwayError), not after waiting for a surviving stream to free a slot (F20)."""
    conn = Connection(None)
    conn.apply_stream_limit(1)
    await conn._acquire_slot()  # occupy the only slot

    raised = Event()

    async with scope() as s:

        async def probe():
            try:
                await conn._acquire_slot()  # parked: no free slot
            except GoAwayError:
                raised.set()

        s.spawn(probe())
        await sleep(0.03)
        assert not raised.is_set()  # still parked — no GOAWAY yet, and no slot freed
        conn._dispatch(_goaway_frame(), None)  # wakes the slot waiter to re-check
        await raised.wait()  # fails promptly, without any slot ever freeing
        s.cancel()
