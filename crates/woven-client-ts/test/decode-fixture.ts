import { readFileSync } from "node:fs";
import { ByteBuffer } from "flatbuffers";
import { DeliveryClass, Envelope, MessageKind } from "../generated/woven/protocol/v1.js";

const frame = readFileSync("../woven-protocol/tests/fixtures/reliable_event_v1.swp");
const envelope = Envelope.getSizePrefixedRootAsEnvelope(new ByteBuffer(new Uint8Array(frame)));

if (envelope.protocolVersion() !== 1) throw new Error("unexpected protocol version");
if (envelope.messageKind() !== MessageKind.ReliableEvent) throw new Error("unexpected message kind");
if (envelope.deliveryClass() !== DeliveryClass.ReliableOrdered) throw new Error("unexpected delivery class");
if (envelope.payloadLength() === 0) throw new Error("missing opaque payload");

console.log("decoded reliable_event_v1.swp");
