export {
  WovenClient,
  type WovenConfig,
  type WovenError,
} from "./client.js";
export { EnvelopeCodec, CodecError, PROTOCOL_VERSION, FILE_IDENTIFIER } from "./codec.js";
export type { DecodedEnvelope } from "./codec.js";
export {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeSubscribeSpace,
  encodeSnapshotRequest,
  encodeSpaceTransition,
  encodeInferenceRequested,
  encodeReliableEvent,
  encodeEntityState,
} from "./encode.js";
export type { EnvelopeScope } from "./encode.js";
export {
  TRANSFORM_ENCODED_LENGTH,
  encodeTransform,
  decodeTransform,
  type Transform,
} from "./transform.js";
export {
  resolveWebTransportConstructor,
  type WebTransport,
  type WebTransportOptions,
  type WebTransportBidirectionalStream,
  type WebTransportDatagramDuplexStream,
  type WebTransportCloseInfo,
} from "./webtransport.js";
