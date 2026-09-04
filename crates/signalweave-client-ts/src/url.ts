/**
 * Resolve a Signalweave server URL to a WHATWG WebTransport URL.
 *
 * The standardized server URL is `quic://host:port`. A browser client only
 * speaks WebTransport, so the `quic://` scheme is derived to a WebTransport
 * endpoint using the deterministic port convention: the server listens for
 * WebTransport one port above the native QUIC port (`host:(port+1)`), on the
 * `/webtransport` path unless the URL carries its own path.
 */

/** Default native QUIC port when the URL omits it. */
const DEFAULT_QUIC_PORT = 4433;

/**
 * Resolve a Signalweave server URL to a WHATWG WebTransport URL.
 *
 * - `wtransport://` / `https://` / `http://` — used as-is (the `wtransport://`
 *   scheme is rewritten to `https://` for the WHATWG API).
 * - `quic://host:port` — derived to WebTransport via the deterministic port
 *   convention (`host:(port+1)` on `/webtransport`).
 */
export function toWebTransportUrl(url: string): string {
  const lower = url.toLowerCase();
  if (lower.startsWith("wtransport://")) {
    return "https://" + url.slice("wtransport://".length);
  }
  if (lower.startsWith("quic://")) {
    return toWebTransportFromQuic(url);
  }
  return url;
}

/**
 * Derive the WebTransport endpoint for a `quic://host:port` URL.
 */
export function toWebTransportFromQuic(url: string): string {
  const rest = url.slice("quic://".length);
  const slash = rest.indexOf("/");
  const authority = slash >= 0 ? rest.slice(0, slash) : rest;
  const pathSpecified = slash >= 0 ? rest.slice(slash) : null;

  let host = authority;
  let port = DEFAULT_QUIC_PORT;
  if (authority.startsWith("[")) {
    const close = authority.indexOf("]");
    if (close > 0) {
      host = authority.slice(0, close + 1);
      const after = authority.slice(close + 1);
      if (after.startsWith(":")) {
        port = Number(after.slice(1));
      }
    }
  } else {
    const colon = authority.lastIndexOf(":");
    if (colon >= 0) {
      host = authority.slice(0, colon);
      port = Number(authority.slice(colon + 1));
    }
  }

  if (!Number.isFinite(port)) {
    port = DEFAULT_QUIC_PORT;
  }
  const path = pathSpecified && pathSpecified !== "/" ? pathSpecified : "/webtransport";
  return `https://${host}:${port + 1}${path}`;
}
