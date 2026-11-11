#!/bin/sh
# Run the PostgreSQL tests and workflow example against an explicitly supplied scratch database
# (AGENT_RS_TEST_POSTGRES_URL), or provision an isolated local one that is removed on exit.
# With arguments, run that command against the database instead.
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
if [ -z "${AGENT_RS_TEST_POSTGRES_URL:-}" ]; then
    command -v initdb >/dev/null
    command -v pg_ctl >/dev/null
    pg_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/agent-rs-pg.XXXXXX")
    cleanup() {
        pg_ctl -D "$pg_test_dir/data" -m immediate -w stop >/dev/null 2>&1 || true
        rm -rf -- "$pg_test_dir"
    }
    trap cleanup EXIT HUP INT TERM
    initdb -D "$pg_test_dir/data" -U agent_rs_test -A trust --no-locale --encoding=UTF8 >"$pg_test_dir/init.log"
    pg_ctl -D "$pg_test_dir/data" -l "$pg_test_dir/server.log" \
        -o "-h '' -k '$pg_test_dir' -p 55454 -c fsync=on" -w start >/dev/null
    export AGENT_RS_TEST_POSTGRES_URL="host=$pg_test_dir port=55454 user=agent_rs_test dbname=postgres"
fi
if [ "$#" -gt 0 ]; then
    "$@"
else
    scripts/rust.sh test --all-features --locked --test storage_conformance --test postgres -- --include-ignored
    scripts/rust.sh run --no-default-features --features postgres --locked --example postgres_workflow
fi
