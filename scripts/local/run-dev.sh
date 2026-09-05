#!/bin/sh
set -eu

SCRIPT_DIR=$(dirname "$0")
REPOSITORY_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

cd "$REPOSITORY_ROOT"
exec cargo run --locked -p woven-server -- "$@"
