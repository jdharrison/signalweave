import { readFileSync } from "node:fs";
import { ByteBuffer } from "flatbuffers";
import {
  ControlPayload,
  Envelope,
  MessageKind,
  ToolCallCompletedPayload,
} from "../generated/woven/protocol/v1.js";

// Proves the TypeScript bindings can decode a Rust-produced typed inference/tool-call
// control message, independent of the ReliableEvent golden fixture.
const frame = readFileSync("../woven-protocol/tests/fixtures/tool_call_completed_v1.swp");
const envelope = Envelope.getSizePrefixedRootAsEnvelope(new ByteBuffer(new Uint8Array(frame)));

if (envelope.protocolVersion() !== 1) throw new Error("unexpected protocol version");
if (envelope.messageKind() !== MessageKind.ToolCallCompleted) {
  throw new Error("unexpected message kind");
}
if (envelope.entityId() !== BigInt(7)) throw new Error("unexpected entity id");

const payload = envelope.control(new ToolCallCompletedPayload());
if (!payload || envelope.controlType() !== ControlPayload.ToolCallCompletedPayload) {
  throw new Error("missing ToolCallCompletedPayload");
}
if (payload.newRevision() !== BigInt(2)) throw new Error("unexpected new_revision");
const result = Buffer.from(payload.resultArray() ?? new Uint8Array()).toString("utf8");
if (result !== "status updated") throw new Error(`unexpected result: ${result}`);

console.log("decoded tool_call_completed_v1.swp");
