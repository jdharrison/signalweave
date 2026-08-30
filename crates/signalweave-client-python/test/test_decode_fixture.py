"""Proves the Python bindings decode the same Rust-produced golden fixtures the
TypeScript and C# clients do, closing the cross-language loop for all reference clients.
"""

from pathlib import Path

from Signalweave.Protocol.V1.ControlPayload import ControlPayload
from Signalweave.Protocol.V1.DeliveryClass import DeliveryClass
from Signalweave.Protocol.V1.Envelope import Envelope
from Signalweave.Protocol.V1.MessageKind import MessageKind
from Signalweave.Protocol.V1.ToolCallCompletedPayload import ToolCallCompletedPayload

FIXTURES = Path(__file__).parent.parent.parent / "signalweave-protocol" / "tests" / "fixtures"

# The wire format is size-prefixed (a 4-byte little-endian length ahead of the root
# table), matching Codec's framing in signalweave-protocol.
SIZE_PREFIX_LEN = 4


def _read_fixture(name: str) -> bytearray:
    return bytearray((FIXTURES / name).read_bytes())


def test_reliable_event_fixture_decodes_to_expected_values():
    data = _read_fixture("reliable_event_v1.swp")
    envelope = Envelope.GetRootAs(data, SIZE_PREFIX_LEN)

    assert envelope.ProtocolVersion() == 1
    assert envelope.MessageKind() == MessageKind.ReliableEvent
    assert envelope.DeliveryClass() == DeliveryClass.ReliableOrdered
    assert envelope.PayloadLength() > 0
    assert envelope.ControlType() == ControlPayload.NONE


def test_tool_call_completed_fixture_decodes_to_expected_values():
    data = _read_fixture("tool_call_completed_v1.swp")
    envelope = Envelope.GetRootAs(data, SIZE_PREFIX_LEN)

    assert envelope.ProtocolVersion() == 1
    assert envelope.MessageKind() == MessageKind.ToolCallCompleted
    assert envelope.EntityId() == 7
    assert envelope.ControlType() == ControlPayload.ToolCallCompletedPayload

    union_table = envelope.Control()
    payload = ToolCallCompletedPayload()
    payload.Init(union_table.Bytes, union_table.Pos)
    assert payload.NewRevision() == 2
    result = bytes(payload.Result(i) for i in range(payload.ResultLength()))
    assert result.decode("utf-8") == "status updated"
