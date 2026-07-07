"""Shared test helper: dial a TCP transport and wrap it in an `H2Connection`.

httpunk's connection API is transport-injected (bring your own connected
transport), so tests dial first — exactly what a caller (or `httpunk.util`,
later) does. Kept out of a fixture so it composes inside `async with scope()`.
"""

import contextlib

from httpunk import H1Connection, H2Connection
from httpunk._backend.tonio import TonioBackend


@contextlib.asynccontextmanager
async def open_h2(host, port, **kwargs):
    transport = await TonioBackend().connect_tcp(host, port)
    async with H2Connection(transport, authority=f"{host}:{port}", **kwargs) as conn:
        yield conn


@contextlib.asynccontextmanager
async def open_h1(host, port, **kwargs):
    transport = await TonioBackend().connect_tcp(host, port)
    async with H1Connection(transport, authority=f"{host}:{port}", **kwargs) as conn:
        yield conn
