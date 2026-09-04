/**
 * Minimal WHATWG WebTransport interfaces, declared locally so the client does
 * not depend on a host's `lib.dom.d.ts`. In the browser these resolve to the
 * standard global `WebTransport` classes.
 */

export interface WebTransportError extends Error {
  source: "stream" | "session";
  streamErrorCode: number | null;
}

export interface WebTransportCloseInfo {
  closeCode?: number;
  reason?: string;
}

export interface WebTransportBidirectionalStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export interface WebTransportDatagramDuplexStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly incomingMaxAge: number | null;
  readonly outgoingMaxAge: number | null;
  readonly incomingHighWaterMark: number;
  readonly outgoingHighWaterMark: number;
}

export interface WebTransportOptions {
  allowPooling?: boolean;
  requireUnreliable?: boolean;
  serverCertificateHashes?: unknown[];
  congestionControl?: "default" | "throughput";
}

/**
 * The subset of the WHATWG `WebTransport` interface used by the client.
 */
export interface WebTransport {
  readonly ready: Promise<void>;
  readonly closed: Promise<WebTransportCloseInfo>;
  readonly datagrams: WebTransportDatagramDuplexStream;
  createBidirectionalStream(): Promise<WebTransportBidirectionalStream>;
  close(info?: WebTransportCloseInfo): void;
}

export interface WebTransportConstructor {
  new (url: string, options?: WebTransportOptions): WebTransport;
}

/**
 * Resolve the global `WebTransport` constructor at runtime, or throw when the
 * environment does not provide it (for example plain Node without a shim).
 */
export function resolveWebTransportConstructor(): WebTransportConstructor {
  const globalObject = globalThis as unknown as {
    WebTransport?: WebTransportConstructor;
  };
  if (typeof globalObject.WebTransport !== "function") {
    throw new Error(
      "WebTransport is not available in this environment. " +
        "Signalweave's TypeScript client requires a browser (or runtime shim) " +
        "that implements the WHATWG WebTransport API.",
    );
  }
  return globalObject.WebTransport;
}
