import { test, describe, beforeEach } from "node:test";
import { strict as assert } from "node:assert";
import { SignalweaveClient } from "../src/client.js";
import { EnvelopeCodec, DecodedEnvelope } from "../src/codec.js";
import { MessageKind, DeliveryClass, ControlPayload } from "../generated/signalweave/protocol/v1.js";
import { Envelope as FbEnvelope } from "../generated/signalweave/protocol/v1/envelope.js";
import * as flatbuffers from "flatbuffers";
import { WebTransport, WebTransportBidirectionalStream } from "../src/webtransport.js";
import {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeSubscribeSpace,
  encodeReliableEvent,
} from "../src/encode.js";

const codec = new EnvelopeCodec();
const encoder = new TextEncoder();

/** Build a minimal valid size-prefixed Envelope frame with just a message kind. */
function buildControlFrame(kind: MessageKind): Uint8Array {
  const builder = new flatbuffers.Builder(128);
  const root = FbEnvelope.createEnvelope(
    builder,
    1,
    kind,
    DeliveryClass.ReliableOrdered,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0,
    ControlPayload.NONE,
    0,
    0n,
  );
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
  return builder.asUint8Array();
}

/**
 * A minimal in-memory WebTransport server that speaks enough of the Signalweave
 * protocol to handshake: on Hello it queues a Capabilities frame, on Authenticate
 * it queues an Authenticated frame, and it records every decoded request.
 */
class FakeServer {
  readonly requests: DecodedEnvelope[] = [];
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly bidi: WebTransportBidirectionalStream;
  private controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  private pushQueue: Uint8Array[] = [];
  private acc = new Uint8Array(0);

  constructor() {
    const self = this;
    this.readable = new ReadableStream<Uint8Array>({
      start: (c) => {
        this.controller = c;
      },
      pull: () => {
        while (this.pushQueue.length > 0) {
          this.controller?.enqueue(this.pushQueue.shift()!);
        }
      },
    });
    this.writable = new WritableStream({
      write(chunk: Uint8Array) {
        self.ingest(new Uint8Array(chunk));
      },
    });
    this.bidi = { readable: this.readable, writable: this.writable };
  }

  pushFrame(frame: Uint8Array): void {
    if (this.controller) this.controller.enqueue(frame);
    else this.pushQueue.push(frame);
  }

  private ingest(chunk: Uint8Array): void {
    const merged = new Uint8Array(this.acc.length + chunk.length);
    merged.set(this.acc);
    merged.set(chunk, this.acc.length);
    this.acc = merged;
    for (;;) {
      const result = codec.decodeStream(this.acc);
      if (result === null) break;
      this.acc = this.acc.subarray(result.consumed);
      const envelope = result.envelope;
      this.requests.push(envelope);
      if (envelope.messageKind === MessageKind.Hello) {
        this.pushFrame(buildControlFrame(MessageKind.Capabilities));
      } else if (envelope.messageKind === MessageKind.Authenticate) {
        this.pushFrame(buildControlFrame(MessageKind.Authenticated));
      }
    }
  }
}

function makeWebTransport(server: FakeServer): WebTransport {
  return {
    ready: Promise.resolve(),
    closed: Promise.resolve({ closeCode: 0 }),
    datagrams: {
      readable: new ReadableStream(),
      writable: new WritableStream(),
      incomingMaxAge: null,
      outgoingMaxAge: null,
      incomingHighWaterMark: 0,
      outgoingHighWaterMark: 0,
    },
    createBidirectionalStream: async () => server.bidi,
    close: () => {},
  } as WebTransport;
}

describe("SignalweaveClient handshake over mocked WebTransport", () => {
  let server: FakeServer;
  let wt: WebTransport;

  beforeEach(() => {
    server = new FakeServer();
    wt = makeWebTransport(server);
  });

  test("completes Hello -> Capabilities -> Authenticate -> Authenticated", async () => {
    const client = await SignalweaveClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    assert.equal(server.requests[0]!.messageKind, MessageKind.Hello);
    assert.equal(server.requests[1]!.messageKind, MessageKind.Authenticate);
    client.close();
  });

  test("joinSession and subscribeSpace are sent and decoded", async () => {
    const client = await SignalweaveClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    await client.joinSession(1n, 1n);
    await client.subscribeSpace(1n, 1n, 1n, 1n, 1n);
    assert.equal(server.requests[2]!.messageKind, MessageKind.JoinSession);
    assert.equal(server.requests[2]!.namespaceId, 1n);
    assert.equal(server.requests[3]!.messageKind, MessageKind.SubscribeSpace);
    assert.equal(server.requests[3]!.spaceId, 1n);
    assert.equal(server.requests[3]!.channelId, 1n);
    client.close();
  });

  test("publishEvent carries the payload and sequence", async () => {
    const client = await SignalweaveClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    await client.publishEvent(1n, 1n, 1n, 1n, 1n, 1n, 1n, 1n, encoder.encode("hi"));
    assert.equal(server.requests[2]!.messageKind, MessageKind.ReliableEvent);
    assert.equal(server.requests[2]!.entityId, 1n);
    assert.equal(server.requests[2]!.senderSequence, 1n);
    assert.deepEqual(server.requests[2]!.payload, encoder.encode("hi"));
    client.close();
  });

  test("recv yields envelopes queued by the server", async () => {
    const client = await SignalweaveClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    server.pushFrame(encodeReliableEvent(
      { namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n, entityId: 5n, senderSequence: 1n },
      { typeId: 1n, bytes: encoder.encode("from-server") },
    ));
    const envelope = await client.recv();
    assert.equal(envelope.messageKind, MessageKind.ReliableEvent);
    assert.equal(envelope.entityId, 5n);
    assert.deepEqual(envelope.payload, encoder.encode("from-server"));
    client.close();
  });
});

describe("SignalweaveClient connect: quic:// derives WebTransport endpoint", () => {
  test("connect maps quic://host:PORT to https://host:(PORT+1)/webtransport", async () => {
    const server = new FakeServer();
    const seenUrl: string[] = [];

    const SpyingWebTransport = function (url: string) {
      seenUrl.push(url);
      return makeWebTransport(server);
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      const client = await SignalweaveClient.connect({
        url: "quic://127.0.0.1:8081",
        token: "dev-token",
      });
      assert.deepEqual(seenUrl, ["https://127.0.0.1:8082/webtransport"]);
      client.close();
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect maps quic:// default port 4433 to 4434", async () => {
    const server = new FakeServer();
    const seenUrl: string[] = [];

    const SpyingWebTransport = function (url: string) {
      seenUrl.push(url);
      return makeWebTransport(server);
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      const client = await SignalweaveClient.connect({
        url: "quic://relay.example",
        token: "dev-token",
      });
      assert.deepEqual(seenUrl, ["https://relay.example:4434/webtransport"]);
      client.close();
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });
});

describe("encode path direct equality with generated bindings", () => {
  test("hello round-trips through the generated Decoder", () => {
    const frame = encodeHello({ clientName: "x", clientVersion: "1" });
    const envelope = codec.decode(frame);
    assert.equal(envelope.messageKind, MessageKind.Hello);
    assert.equal(envelope.controlType, ControlPayload.HelloPayload);
  });

  test("authenticate round-trips through the generated Decoder", () => {
    const frame = encodeAuthenticate(encoder.encode("token"));
    const envelope = codec.decode(frame);
    assert.equal(envelope.messageKind, MessageKind.Authenticate);
    assert.equal(envelope.controlType, ControlPayload.AuthenticatePayload);
  });
});
