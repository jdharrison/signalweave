using System.Diagnostics;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using Google.FlatBuffers;
using Signalweave.Protocol.V1;

namespace SignalweaveClientCSharp.Tests;

/// Spawns the real Rust server and proves the C# bindings can decode a live binary
/// WebSocket frame it produces, closing the cross-language loop end to end rather than
/// only against checked-in fixtures.
public class DecodeLiveServerTests
{
    private static string RepoRoot([CallerFilePath] string sourceFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(sourceFile)!, "..", "..", ".."));

    [Fact]
    public async Task MalformedFrameReceivesProtocolErrorFromLiveServer()
    {
        using var server = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "cargo",
                Arguments = "run -p signalweave-server",
                WorkingDirectory = RepoRoot(),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            },
        };
        server.Start();
        try
        {
            var frame = await ReceiveServerFrameAsync();
            // Size-prefixed wire format; see DecodeFixtureTests.SizePrefixedEnvelope.
            var envelope = Envelope.GetRootAsEnvelope(new ByteBuffer(frame, FlatBufferConstants.SizePrefixLength));
            Assert.Equal(MessageKind.ProtocolError, envelope.MessageKind);

            var payload = envelope.ControlAsProtocolErrorPayload();
            Assert.Equal(ProtocolErrorCode.MalformedFrame, payload.Code);
        }
        finally
        {
            server.Kill(entireProcessTree: true);
            await server.WaitForExitAsync();
        }
    }

    private static async Task<byte[]> ReceiveServerFrameAsync()
    {
        // Generous window: a cold `cargo run` (uncached CI dependencies) can take well over
        // a minute before the server is listening, even though a pre-warmed local build is
        // instant.
        Exception lastError = null;
        for (var attempt = 0; attempt < 300; attempt++)
        {
            try
            {
                using var socket = new ClientWebSocket();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await socket.ConnectAsync(new Uri("ws://127.0.0.1:8080/ws"), cts.Token);
                await socket.SendAsync(new byte[] { 1, 2, 3 }, WebSocketMessageType.Binary, true, cts.Token);

                var buffer = new byte[4096];
                var result = await socket.ReceiveAsync(buffer, cts.Token);
                return buffer[..result.Count];
            }
            catch (Exception error)
            {
                lastError = error;
                await Task.Delay(200);
            }
        }
        throw new TimeoutException("Rust server did not accept a WebSocket connection", lastError);
    }
}
