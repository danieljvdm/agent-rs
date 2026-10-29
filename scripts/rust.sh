#!/bin/sh
# Select the project's pinned toolchain even when Homebrew cargo precedes rustup.
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
agent_rust_bin=$(dirname "$(rustup which cargo)")
export PATH="$agent_rust_bin:$PATH"
exec cargo "$@"
