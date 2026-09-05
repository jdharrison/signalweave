#!/bin/sh
set -eu

for command in cargo rustc cmake cc; do
    if ! command -v "$command" >/dev/null 2>&1; then
        printf 'missing required command: %s\n' "$command" >&2
        exit 1
    fi
done

printf 'cargo: '
cargo --version
printf 'rustc: '
rustc --version
printf 'cmake: '
cmake --version | sed -n '1p'
printf 'C compiler: '
cc --version | sed -n '1p'

if command -v node >/dev/null 2>&1; then
    printf 'node (needed for the TypeScript client): '
    node --version
else
    printf 'node: not found (only required for TypeScript client development)\n'
fi

printf '%s\n' 'Local prerequisites are available.'
