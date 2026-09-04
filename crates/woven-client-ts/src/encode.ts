import * as flatbuffers from "flatbuffers";
import {
  Envelope as FbEnvelope,
  HelloPayload,
  AuthenticatePayload,
  JoinSessionPayload,
  SubscribeSpacePayload,
  SnapshotRequestPayload,
  SpaceTransitionPayload,
  InferenceRequestedPayload,
  AuthenticationScheme,
  ControlPayload,
  DeliveryClass,
  MessageKind,
} from "../generated/woven/protocol/v1.js";
import { PROTOCOL_VERSION } from "./codec.js";

/** Options shared by every outbound message. */
export interface EnvelopeScope {
  namespaceId?: bigint;
  sessionId?: bigint;
  spaceId?: bigint;
  spaceEpoch?: bigint;
  channelId?: bigint;
  entityId?: bigint;
  senderSequence?: bigint;
}

const zero = 0n;

/**
 * Encode a Hello control envelope as a size-prefixed frame. Mirrors the Rust
 * reference client's handshake start.
 */
export function encodeHello(opts: {
  clientName?: string;
  clientVersion?: string;
  maxFrameSize?: number;
  maxPayloadSize?: number;
}): Uint8Array {
  const builder = new flatbuffers.Builder(1024);
  const clientName = builder.createString(opts.clientName ?? "woven-client-ts");
  const clientVersion = builder.createString(opts.clientVersion ?? "0.1.0");
  const control = HelloPayload.createHelloPayload(
    builder,
    1,
    1,
    clientName,
    clientVersion,
    zero,
    opts.maxFrameSize ?? 65536,
    opts.maxPayloadSize ?? 65536,
  );
  return finishControl(builder, MessageKind.Hello, ControlPayload.HelloPayload, control, {
    deliveryClass: DeliveryClass.ReliableOrdered,
  });
}

/** Encode an Authenticate control envelope. */
export function encodeAuthenticate(credentials: Uint8Array): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const creds = builder.createByteVector(credentials);
  const control = AuthenticatePayload.createAuthenticatePayload(
    builder,
    AuthenticationScheme.Development,
    creds,
  );
  return finishControl(
    builder,
    MessageKind.Authenticate,
    ControlPayload.AuthenticatePayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered },
  );
}

/**
 * Encode a JoinSession control envelope.
 *
 * JoinSession is session-scoped, so `namespaceId` and `sessionId` must be nonzero.
 */
export function encodeJoinSession(
  scope: { namespaceId: bigint; sessionId: bigint },
  resumeToken?: Uint8Array,
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const token = builder.createByteVector(resumeToken ?? new Uint8Array(0));
  const control = JoinSessionPayload.createJoinSessionPayload(builder, token);
  return finishControl(
    builder,
    MessageKind.JoinSession,
    ControlPayload.JoinSessionPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a SubscribeSpace control envelope. */
export function encodeSubscribeSpace(
  scope: EnvelopeScope & { channelId: bigint },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SubscribeSpacePayload.createSubscribeSpacePayload(
    builder,
    scope.channelId,
  );
  return finishControl(
    builder,
    MessageKind.SubscribeSpace,
    ControlPayload.SubscribeSpacePayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a SnapshotRequest control envelope. */
export function encodeSnapshotRequest(
  scope: EnvelopeScope & { channelId: bigint },
  afterServerTick?: bigint,
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SnapshotRequestPayload.createSnapshotRequestPayload(
    builder,
    scope.channelId,
    afterServerTick ?? zero,
  );
  return finishControl(
    builder,
    MessageKind.SnapshotRequest,
    ControlPayload.SnapshotRequestPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a SpaceTransition control envelope. */
export function encodeSpaceTransition(
  scope: EnvelopeScope & { entityId: bigint },
  opts: { fromSpaceId: bigint; toSpaceId: bigint; toSpaceEpoch: bigint },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SpaceTransitionPayload.createSpaceTransitionPayload(
    builder,
    opts.fromSpaceId,
    opts.toSpaceId,
    opts.toSpaceEpoch,
  );
  return finishControl(
    builder,
    MessageKind.SpaceTransition,
    ControlPayload.SpaceTransitionPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode an InferenceRequested control envelope. */
export function encodeInferenceRequested(
  scope: EnvelopeScope & { entityId: bigint },
  opts: { capability: string; deadlineMs: bigint; input: Uint8Array },
): Uint8Array {
  const builder = new flatbuffers.Builder(1024);
  const capability = builder.createString(opts.capability);
  const input = builder.createByteVector(opts.input);
  const control = InferenceRequestedPayload.createInferenceRequestedPayload(
    builder,
    capability,
    opts.deadlineMs,
    input,
  );
  return finishControl(
    builder,
    MessageKind.InferenceRequested,
    ControlPayload.InferenceRequestedPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a reliable opaque event payload envelope. */
export function encodeReliableEvent(
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  return encodeOpaque(
    MessageKind.ReliableEvent,
    DeliveryClass.ReliableOrdered,
    scope,
    payload,
  );
}

/** Encode a latest-value (entity state) opaque payload envelope. */
export function encodeEntityState(
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  return encodeOpaque(
    MessageKind.EntityState,
    DeliveryClass.LatestValue,
    scope,
    payload,
  );
}

function encodeOpaque(
  kind: MessageKind,
  deliveryClass: DeliveryClass,
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  const builder = new flatbuffers.Builder(1024);
  const payloadOffset = builder.createByteVector(payload.bytes);
  const root = FbEnvelope.createEnvelope(
    builder,
    PROTOCOL_VERSION,
    kind,
    deliveryClass,
    scope.namespaceId ?? zero,
    scope.sessionId ?? zero,
    scope.spaceId ?? zero,
    scope.entityId ?? zero,
    scope.spaceEpoch ?? zero,
    zero,
    scope.senderSequence ?? zero,
    zero,
    payload.typeId,
    payloadOffset,
    ControlPayload.NONE,
    0,
    scope.channelId ?? zero,
  );
  finishEnvelope(builder, root);
  return builder.asUint8Array();
}

function finishControl(
  builder: flatbuffers.Builder,
  kind: MessageKind,
  controlType: ControlPayload,
  controlOffset: number,
  opts: { deliveryClass: DeliveryClass; scope?: EnvelopeScope },
): Uint8Array {
  const scope = opts.scope ?? {};
  const root = FbEnvelope.createEnvelope(
    builder,
    PROTOCOL_VERSION,
    kind,
    opts.deliveryClass,
    scope.namespaceId ?? zero,
    scope.sessionId ?? zero,
    scope.spaceId ?? zero,
    scope.entityId ?? zero,
    scope.spaceEpoch ?? zero,
    zero,
    scope.senderSequence ?? zero,
    zero,
    zero,
    0,
    controlType,
    controlOffset,
    scope.channelId ?? zero,
  );
  finishEnvelope(builder, root);
  return builder.asUint8Array();
}

function finishEnvelope(
  builder: flatbuffers.Builder,
  root: number,
): void {
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
}
