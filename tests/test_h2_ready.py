"""`H2Connection.ready()` / `StreamManager.wait_until_ready` — verify it really
*awaits* for a MAX_CONCURRENT_STREAMS slot (h2 `SendRequest::ready`), rather than
returning synchronously. These are socket-free unit tests against the mechanism."""

import pytest
from tonio.colored import Event, scope, sleep

from httpunk import GoAwayError, H2Reason
from httpunk.h2.connection import Connection


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
