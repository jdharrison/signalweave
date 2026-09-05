#!/bin/sh
set -eu

SCRIPT_DIR=$(dirname "$0")
REPOSITORY_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

cat <<'NOTICE'
This runs the release-mode direct-core routing benchmark.
It measures core routing and bounded outbound queues only; it does not exercise
QUIC, WebTransport, sockets, TLS, or the shared transport worker command queue.
NOTICE

cd "$REPOSITORY_ROOT"
exec cargo run --locked --release -p woven-loadtest -- "$@"
