"""Shared tonio test helpers: loopback listeners and pairs, dial-and-wrap, raw reads,
and a raw HTTP/2 peer.

httpunk's connection API is transport-injected (bring your own connected
transport), so tests dial first — exactly what a caller (or `httpunk.util`,
later) does. Kept out of a fixture so it composes inside `async with scope()`.
"""

import collections
import contextlib
from typing import NamedTuple

from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners
from tonio.exceptions import CancelledError

from httpunk import H1Connection, H2Connection
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import H2Codec, H2FrameData, H2FrameHeaders, H2FramePing, H2FrameSettings
from httpunk.h2.connection import PREFACE


async def listen():
    """A loopback TCP listener: `(listener, host, port)`."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


@contextlib.asynccontextmanager
async def open_h2(host, port, **kwargs):
    transport = await TonioBackend().connect_tcp(host, port)
    kwargs.setdefault("authority", f"{host}:{port}")
    async with H2Connection(transport, **kwargs) as conn:
        yield conn


@contextlib.asynccontextmanager
async def open_h1(host, port, **kwargs):
    transport = await TonioBackend().connect_tcp(host, port)
    async with H1Connection(transport, authority=f"{host}:{port}", **kwargs) as conn:
        yield conn


@contextlib.asynccontextmanager
async def tcp_pair():
    """A connected loopback pair `(client, peer)`, both closed on exit."""
    listener, host, port = await listen()
    client = await TonioBackend().connect_tcp(host, port)
    peer = await listener.accept()
    listener.close()
    try:
        yield client, peer
    finally:
        client.close()
        peer.close()


async def read_head(stream):
    """Read until a message head ends: the bytes read (the head plus whatever followed
    it in the last chunk), or None on EOF first."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = await stream.receive_some(65536)
        if not chunk:
            return None
        buf += chunk
    return buf


async def read_to_eof(stream, *, allow_reset=False):
    """Read until EOF and return the bytes. `allow_reset`: a reset also ends the read
    (an abortive close with unread input may send RST rather than FIN)."""
    data = b""
    try:
        while chunk := await stream.receive_some(65536):
            data += chunk
    except TonioBackend.broken_transport_errors:
        if not allow_reset:
            raise
    return data


class H2Request(NamedTuple):
    stream_id: int
    headers: H2FrameHeaders
    data: list[H2FrameData]


class H2Peer:
    """A raw HTTP/2 endpoint over a connected stream. Reads are buffered (one receive
    may carry several frames); the peer's SETTINGS are ACKed as they are read, kept in
    `settings` and not returned. `send_<frame>(...)` sends `codec.serialize_<frame>(...)`."""

    def __init__(self, stream, role):
        self.stream = stream
        self.codec = H2Codec(role)
        self.settings = None
        self._pending = collections.deque()

    @classmethod
    async def client(cls, stream, **settings):
        """The client side: send the preface and SETTINGS."""
        peer = cls(stream, "client")
        await peer.send(PREFACE, peer.codec.serialize_settings(**settings))
        return peer

    @classmethod
    async def connect(cls, host, port, **settings):
        return await cls.client(await TonioBackend().connect_tcp(host, port), **settings)

    @classmethod
    async def server(cls, stream, **settings):
        """The server side: send SETTINGS, then read the client's preface."""
        peer = cls(stream, "server")
        await peer.send(peer.codec.serialize_settings(**settings))
        preface = b""
        while len(preface) < len(PREFACE):
            chunk = await stream.receive_some(len(PREFACE) - len(preface))
            assert chunk, "the connection closed before the preface"
            preface += chunk
        assert preface == PREFACE
        return peer

    @classmethod
    async def accept(cls, listener, **settings):
        return await cls.server(await listener.accept(), **settings)

    def __getattr__(self, name):
        if not name.startswith("send_"):
            raise AttributeError(name)
        serialize = getattr(self.codec, "serialize_" + name.removeprefix("send_"))

        async def send(*args, **kwargs):
            await self.send(serialize(*args, **kwargs))

        return send

    async def send(self, *frames):
        await self.stream.send_all(b"".join(frames))

    async def recv(self, *kinds):
        """The next frame (the next of one of `kinds`, skipping others), or None at EOF."""
        while True:
            while self._pending:
                frame = self._pending.popleft()
                if isinstance(frame, H2FrameSettings) and not frame.ack:
                    self.settings = frame
                    await self.send(self.codec.serialize_settings_ack())
                elif not kinds or isinstance(frame, kinds):
                    return frame
            data = await self.stream.receive_some(65536)
            if not data:
                return None
            self._pending.extend(self.codec.receive(data))

    async def expect(self, kind, *skip):
        """The next frame, skipping frames of the `skip` types: asserted to be a `kind`."""
        while isinstance(frame := await self.recv(), skip):
            pass
        assert isinstance(frame, kind), f"expected {kind.__name__}, got {frame!r}"
        return frame

    async def frames(self):
        """Every frame until EOF."""
        frames = []
        while (frame := await self.recv()) is not None:
            frames.append(frame)
        return frames

    async def handshake(self):
        """Wait for the ACK of our SETTINGS; frames read before it stay queued. Returns
        the peer's SETTINGS."""
        held = []
        while not isinstance(frame := await self.recv(), H2FrameSettings):
            assert frame is not None, "the connection closed before the SETTINGS ACK"
            held.append(frame)
        self._pending.extendleft(reversed(held))
        return self.settings

    async def request(self):
        """Read one request through its END_STREAM (on HEADERS, or on the last DATA),
        skipping other frames."""
        headers = await self.recv(H2FrameHeaders)
        assert headers is not None, "the connection closed before a request"
        data = []
        while not (data[-1] if data else headers).end_stream:
            frame = await self.recv(H2FrameData)
            assert frame is not None, "the connection closed mid-request"
            data.append(frame)
        return H2Request(headers.stream_id, headers, data)

    async def ping_pong(self, payload=b"\x01" * 8):
        """PING, then assert the very next frame is its ACK."""
        await self.send_ping(payload)
        pong = await self.expect(H2FramePing)
        assert pong.ack and pong.data == payload

    def close(self):
        self.stream.close()


@contextlib.asynccontextmanager
async def _client_against(open_client, serve, conn_kwargs):
    listener, host, port = await listen()
    done = Event()
    failure = []

    async def run():
        try:
            transport = await listener.accept()
            try:
                await serve(transport)
            except CancelledError:
                raise
            except BaseException as exc:  # includes pytest's outcome exceptions
                failure.append(exc)
                transport.close()  # the client sees EOF instead of waiting on a dead server
        finally:
            done.set()

    async with scope() as s:
        s.spawn(run())
        try:
            async with open_client(host, port, **conn_kwargs) as conn:
                yield conn
            await done.wait()
        except BaseException as exc:
            s.cancel()
            if failure:
                raise failure[0] from exc
            raise
    if failure:
        raise failure[0]


def h1_client_against(serve, **conn_kwargs):
    """An `H1Connection` (`open_h1(**conn_kwargs)`) to a server task running
    `serve(transport)` on the accepted connection. On exit the connection closes, then
    the task is awaited. A failure on either side unblocks the other (a failed task
    closes its transport, a failed body cancels the task), and a task failure is what
    gets raised."""
    return _client_against(open_h1, serve, conn_kwargs)


def h2_client_against(script, **conn_kwargs):
    """`h1_client_against` for HTTP/2: an `H2Connection` (`open_h2(**conn_kwargs)`) to a
    raw `H2Peer` server running `script(peer)`."""

    async def serve(transport):
        await script(await H2Peer.server(transport))

    return _client_against(open_h2, serve, conn_kwargs)


@contextlib.asynccontextmanager
async def _peer_against(serve, connect):
    listener, host, port = await listen()

    async def run():
        await serve(await listener.accept())

    async with scope() as s:
        s.spawn(run())
        peer = await connect(host, port)
        try:
            yield peer
        finally:
            peer.close()
            s.cancel()


def h1_peer_against(serve):
    """A raw client transport connected to a server task running `serve(transport)`. On
    exit the transport closes and the task is cancelled: a failure while the connection
    is open would otherwise leave the server parked in a read only our EOF can end, and
    the scope join waiting on it forever."""
    return _peer_against(serve, TonioBackend().connect_tcp)


def h2_peer_against(serve, **settings):
    """`h1_peer_against` for HTTP/2: the client is a raw `H2Peer` (SETTINGS `settings`,
    ENABLE_PUSH=0 by default)."""
    settings.setdefault("enable_push", False)
    return _peer_against(serve, lambda host, port: H2Peer.connect(host, port, **settings))
