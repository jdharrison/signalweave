import { test, describe } from "node:test";
import { strict as assert } from "node:assert";
import { toWebTransportUrl } from "../src/url.js";

describe("toWebTransportUrl URL resolution", () => {
  test("quic:// derives WebTransport one port above, default path", () => {
    assert.equal(
      toWebTransportUrl("quic://127.0.0.1:10000"),
      "https://127.0.0.1:10001/webtransport",
    );
  });

  test("quic:// with a custom path keeps the path", () => {
    assert.equal(
      toWebTransportUrl("quic://host.example:4433/wt"),
      "https://host.example:4434/wt",
    );
  });

  test("quic:// with default port uses 4433 -> 4434", () => {
    assert.equal(toWebTransportUrl("quic://host.example"), "https://host.example:4434/webtransport");
  });

  test("quic:// reliably maps a deterministic sequence across ports", () => {
    for (let p = 8080; p <= 8090; p += 1) {
      assert.equal(
        toWebTransportUrl(`quic://127.0.0.1:${p}`),
        `https://127.0.0.1:${p + 1}/webtransport`,
      );
    }
  });

  test("quic:// IPv6 bracket form maps host and port", () => {
    assert.equal(
      toWebTransportUrl("quic://[::1]:9000"),
      "https://[::1]:9001/webtransport",
    );
  });

  test("wtransport:// rewrites to https://", () => {
    assert.equal(
      toWebTransportUrl("wtransport://127.0.0.1:10001/webtransport"),
      "https://127.0.0.1:10001/webtransport",
    );
  });

  test("explicit https:// URL passes through unchanged", () => {
    assert.equal(
      toWebTransportUrl("https://127.0.0.1:10001/webtransport"),
      "https://127.0.0.1:10001/webtransport",
    );
  });
});
