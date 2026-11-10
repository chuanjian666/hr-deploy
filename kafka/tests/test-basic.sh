#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

assert_eq() {
    [[ "$1" == "$2" ]] || { echo "assertion failed: expected '$2', got '$1'" >&2; exit 1; }
}

make_config() {
    cp "$ROOT/config/kafka-admin.conf.example" "$TMP/kafka-admin.conf"
    KAFKA_CONFIG_FILE="$TMP/kafka-admin.conf" "$ROOT/kafka-admin.sh" config set CONTROLLER_HOSTS "10.20.0.11,10.20.0.12,10.20.0.13" >/dev/null
    KAFKA_CONFIG_FILE="$TMP/kafka-admin.conf" "$ROOT/kafka-admin.sh" config set BROKER_HOSTS "10.20.0.11,10.20.0.12,10.20.0.13" >/dev/null
}

manifest_identity() {
    python3 - "$TMP/cluster/manifest.json" <<'PY'
import json, sys
m=json.load(open(sys.argv[1], encoding="utf-8"))
print(m["cluster"]["id"]+"|"+",".join(n["directoryId"] for n in m["controllers"]))
PY
}

make_config
export KAFKA_CONFIG_FILE="$TMP/kafka-admin.conf"
export KAFKA_CLUSTER_DIR="$TMP/cluster"
export KAFKA_INTERACTIVE=false

"$ROOT/kafka-admin.sh" config validate >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
first_identity="$(manifest_identity)"
"$ROOT/kafka-admin.sh" cluster init >/dev/null
assert_eq "$(manifest_identity)" "$first_identity"
assert_eq "$(find "$TMP/cluster/nodes" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')" "6"
grep -q '^auto.create.topics.enable=false$' "$TMP/cluster/nodes/broker-1-10.20.0.11/config/server.properties"

"$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE minimal >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
# shellcheck disable=SC1090
source "$TMP/cluster/nodes/controller-1-10.20.0.11/.env"
assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "1/2g/-Xms1g -Xmx1g"
# shellcheck disable=SC1090
source "$TMP/cluster/nodes/broker-1-10.20.0.11/.env"
assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "2/10g/-Xms4g -Xmx4g"

# Invalid topology must fail.
perl -0pi -e 's/CONTROLLER_HOSTS=.*/CONTROLLER_HOSTS=10.20.0.11,10.20.0.12/' "$TMP/kafka-admin.conf"
if "$ROOT/kafka-admin.sh" config validate >/dev/null 2>&1; then
    echo "invalid two-controller topology unexpectedly passed" >&2
    exit 1
fi

# Syntax-check all source and generated Bash scripts.
while IFS= read -r -d '' file; do bash -n "$file"; done < <(
    find "$ROOT" -type f -name '*.sh' -print0
)
while IFS= read -r -d '' file; do bash -n "$file"; done < <(
    find "$TMP/cluster/nodes" -type f -name '*.sh' -print0
)

if grep -R -E '\{\{[A-Z_]+\}\}' "$TMP/cluster" >/dev/null; then
    echo "generated files contain unresolved template variables" >&2
    exit 1
fi

echo "Kafka basic tests passed"
