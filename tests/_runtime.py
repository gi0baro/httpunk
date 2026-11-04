"""Shared test helpers for tests that run the same body on both backends.

tonio is imported lazily (`importlib`): a test module that only goes through these
helpers does not match `conftest._NEEDS_TONIO`, so its asyncio twins still run on a
GIL build, where tonio cannot be imported.
"""

import asyncio
import importlib
from contextlib import asynccontextmanager

from httpunk._backend.asyncio import _AsyncioStream


@asynccontextmanager
async def connected_pair(runtime, backend):
    """A connected loopback TCP pair on `runtime` ("tonio" | "asyncio"): yields
    `(client, peer)` transports and closes both, then the listener, on exit."""
    accepted = backend.event()
    peers = []
    if runtime == "tonio":
        net = importlib.import_module("tonio.colored.net")
        listener = (await net.open_tcp_listeners(0, host="127.0.0.1"))[0]
        host, port = listener.socket.getsockname()[:2]

        async def accept():
            peers.append(await listener.accept())
            accepted.set()

        handle = backend.spawn_without_results(accept())
    else:

        def accept():
            peer = _AsyncioStream()
            peers.append(peer)
            accepted.set()
            return peer

        listener = await asyncio.get_running_loop().create_server(accept, "127.0.0.1", 0)
        host, port = listener.sockets[0].getsockname()[:2]
        handle = None
    try:
        client = await backend.connect_tcp(host, port)
        await accepted.wait()
        if handle is not None:
            await handle
        try:
            yield client, peers[0]
        finally:
            backend.close_transport(client)
            backend.close_transport(peers[0])
    finally:
        listener.close()
        if runtime == "asyncio":
            await listener.wait_closed()


def cancelled_type(runtime):
    """The cancellation exception `runtime` raises into a cancelled task."""
    if runtime == "asyncio":
        return asyncio.CancelledError
    return importlib.import_module("tonio.exceptions").CancelledError
