"""Spawns the real Rust server and proves the Python bindings can decode a live binary
WebSocket frame it produces, closing the cross-language loop end to end rather than only
against checked-in fixtures.
"""

import asyncio
import subprocess
from pathlib import Path

import websockets

from Signalweave.Protocol.V1.Envelope import Envelope
from Signalweave.Protocol.V1.MessageKind import MessageKind
from Signalweave.Protocol.V1.ProtocolErrorCode import ProtocolErrorCode
from Signalweave.Protocol.V1.ProtocolErrorPayload import ProtocolErrorPayload

REPO_ROOT = Path(__file__).parent.parent.parent.parent
SIZE_PREFIX_LEN = 4


async def _receive_server_frame() -> bytes:
    # Generous window: a cold `cargo run` (uncached CI dependencies) can take well over a
    # minute before the server is listening, even though a pre-warmed local build is
    # instant.
    last_error: Exception | None = None
    for _ in range(300):
        try:
            async with websockets.connect("ws://127.0.0.1:8080/ws", open_timeout=2) as socket:
                await socket.send(bytes([1, 2, 3]))
                return await asyncio.wait_for(socket.recv(), timeout=2)
        except Exception as error:  # noqa: BLE001 - broad by design; this is a connect retry loop
            last_error = error
            await asyncio.sleep(0.2)
    raise TimeoutError("Rust server did not accept a WebSocket connection") from last_error


def test_malformed_frame_receives_protocol_error_from_live_server():
    server = subprocess.Popen(
        ["cargo", "run", "-p", "signalweave-server"],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        frame = bytearray(asyncio.run(_receive_server_frame()))
        envelope = Envelope.GetRootAs(frame, SIZE_PREFIX_LEN)
        assert envelope.MessageKind() == MessageKind.ProtocolError

        union_table = envelope.Control()
        payload = ProtocolErrorPayload()
        payload.Init(union_table.Bytes, union_table.Pos)
        assert payload.Code() == ProtocolErrorCode.MalformedFrame
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
