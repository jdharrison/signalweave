import { spawn } from "node:child_process";
import { once } from "node:events";
import { resolve } from "node:path";
import { ByteBuffer } from "flatbuffers";
import WebSocket from "ws";
import { Envelope, MessageKind, ProtocolErrorCode, ProtocolErrorPayload } from "../generated/signalweave/protocol/v1.js";

const workspace = resolve(import.meta.dirname, "../../..");
const server = spawn("cargo", ["run", "-p", "signalweave-server"], {
  cwd: workspace,
  stdio: "ignore",
});

try {
  const response = await receiveServerFrame();
  const envelope = Envelope.getSizePrefixedRootAsEnvelope(new ByteBuffer(response));
  if (envelope.messageKind() !== MessageKind.ProtocolError) {
    throw new Error("server did not send a ProtocolError");
  }
  const control = envelope.control(new ProtocolErrorPayload());
  if (!control || control.code() !== ProtocolErrorCode.MalformedFrame) {
    throw new Error("unexpected ProtocolError payload");
  }
  console.log("decoded live Rust server ProtocolError frame");
} finally {
  server.kill("SIGTERM");
  await Promise.race([once(server, "exit"), new Promise((resolve) => setTimeout(resolve, 1000))]);
}

async function receiveServerFrame(): Promise<Uint8Array> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    try {
      return await new Promise<Uint8Array>((resolveFrame, reject) => {
        const socket = new WebSocket("ws://127.0.0.1:8080/ws");
        socket.once("open", () => socket.send(Buffer.from([1, 2, 3])));
        socket.once("message", (data) => {
          socket.close();
          resolveFrame(new Uint8Array(data as Buffer));
        });
        socket.once("error", reject);
      });
    } catch {
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
    }
  }
  throw new Error("Rust server did not accept a WebSocket connection");
}
