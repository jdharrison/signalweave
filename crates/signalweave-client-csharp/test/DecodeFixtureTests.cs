using System.IO;
using System.Text;
using Google.FlatBuffers;
using Signalweave.Protocol.V1;

namespace SignalweaveClientCSharp.Tests;

/// Proves the C# bindings decode the same Rust-produced golden fixtures the TypeScript
/// client does, closing the cross-language loop for all three reference clients.
public class DecodeFixtureTests
{
    private static string FixturePath(string name) =>
        Path.Combine(AppContext.BaseDirectory, "fixtures", name);

    /// The wire format is size-prefixed (a 4-byte little-endian length ahead of the root
    /// table), matching `Codec`'s framing in signalweave-protocol. This flatc version's C#
    /// output has no `GetSizePrefixedRootAsEnvelope` helper (unlike Rust/TypeScript), so the
    /// prefix is skipped explicitly by starting the buffer's position past it.
    private static Envelope SizePrefixedEnvelope(byte[] bytes) =>
        Envelope.GetRootAsEnvelope(new ByteBuffer(bytes, FlatBufferConstants.SizePrefixLength));

    [Fact]
    public void ReliableEventFixtureDecodesToExpectedValues()
    {
        var bytes = File.ReadAllBytes(FixturePath("reliable_event_v1.swp"));
        var envelope = SizePrefixedEnvelope(bytes);

        Assert.Equal((ushort)1, envelope.ProtocolVersion);
        Assert.Equal(MessageKind.ReliableEvent, envelope.MessageKind);
        Assert.Equal(DeliveryClass.ReliableOrdered, envelope.DeliveryClass);
        Assert.True(envelope.PayloadLength > 0);
        Assert.Equal(ControlPayload.NONE, envelope.ControlType);
    }

    [Fact]
    public void ToolCallCompletedFixtureDecodesToExpectedValues()
    {
        var bytes = File.ReadAllBytes(FixturePath("tool_call_completed_v1.swp"));
        var envelope = SizePrefixedEnvelope(bytes);

        Assert.Equal((ushort)1, envelope.ProtocolVersion);
        Assert.Equal(MessageKind.ToolCallCompleted, envelope.MessageKind);
        Assert.Equal((ulong)7, envelope.EntityId);
        Assert.Equal(ControlPayload.ToolCallCompletedPayload, envelope.ControlType);

        var payload = envelope.ControlAsToolCallCompletedPayload();
        Assert.Equal((ulong)2, payload.NewRevision);
        var result = Encoding.UTF8.GetString(payload.GetResultArray() ?? []);
        Assert.Equal("status updated", result);
    }
}
