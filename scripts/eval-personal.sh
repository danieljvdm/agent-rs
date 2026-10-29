#!/bin/sh
# Build without credentials, then inject the existing Yielded development secrets.
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
agent_build_json=$(mktemp)
trap 'rm -f "$agent_build_json"' EXIT HUP INT TERM
scripts/rust.sh build --locked --release --example personal_workflow_eval --message-format=json > "$agent_build_json"
AGENT_EVAL_BIN=$(python3 - "$agent_build_json" <<'PY'
import json, sys
paths = []
for line in open(sys.argv[1]):
    value = json.loads(line)
    if value.get("reason") == "compiler-artifact" and value.get("target", {}).get("name") == "personal_workflow_eval" and value.get("executable"):
        paths.append(value["executable"])
if len(paths) != 1:
    raise SystemExit("Cargo did not produce exactly one workflow eval binary")
print(paths[0])
PY
)
export AGENT_EVAL_BIN
rm -f "$agent_build_json"
trap - EXIT HUP INT TERM
# direnv selects the Yielded machine identity from this project's physical path.
exec direnv exec . sh -eu -c '
    : "${INFISICAL_UNIVERSAL_AUTH_CLIENT_ID:?Missing scoped Infisical client ID}"
    : "${INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET:?Missing scoped Infisical client secret}"
    unset INFISICAL_TOKEN INFISICAL_PROFILE INFISICAL_ORG
    INFISICAL_TOKEN=$(infisical login --method=universal-auth --plain --silent)
    test -n "$INFISICAL_TOKEN"
    export INFISICAL_TOKEN
    exec infisical run --silent \
        --projectId="e27bd4e8-050b-4b7b-ab72-045abb9463cc" --env="dev" --path="/" \
        --domain="https://app.infisical.com/api" -- \
        "$AGENT_EVAL_BIN" --mode live "$@"
' sh "$@"
