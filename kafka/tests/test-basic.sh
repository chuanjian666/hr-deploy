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

# The test profile permits a small disk without changing cluster identities or replication.
"$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE test >/dev/null
"$ROOT/kafka-admin.sh" config set EXPECTED_DATA_DISK_GB 20 >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
assert_eq "$(manifest_identity)" "$first_identity"
python3 - "$TMP/cluster" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / "manifest.json").read_text())
assert manifest["profile"] == "test"
assert manifest["storage"]["diskGB"] == 20
assert len(manifest["controllers"]) == len(manifest["brokers"]) == 3
nodes = list((root / "nodes").iterdir())
assert len(nodes) == 6
for node in nodes:
    assert json.loads((node / "manifest.json").read_text())["profile"] == "test"
    props = (node / "config/server.properties").read_text()
    if node.name.startswith("broker-"):
        for setting in ("default.replication.factor=3", "min.insync.replicas=2",
                        "offsets.topic.replication.factor=3", "transaction.state.log.replication.factor=3",
                        "transaction.state.log.min.isr=2", "auto.create.topics.enable=false"):
            assert setting in props.splitlines(), (node, setting)
assert "不用于生产" in (root / "README.md").read_text()
PY
for role in controller broker; do
    for index in 1 2 3; do
        source "$TMP/cluster/nodes/$role-$index-10.20.0.$((10+index))/.env"
        assert_eq "$DEPLOY_PROFILE/$EXPECTED_DATA_DISK_GB" "test/20"
        if [[ "$role" == controller ]]; then
            assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "0.5/512m/-Xms256m -Xmx256m"
        else
            assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "1/1536m/-Xms512m -Xmx512m"
        fi
    done
done

# Small disks must still be rejected for production; rejected init must preserve existing packages.
before_rejection="$(sha256sum "$TMP/cluster/manifest.json")"
for profile in standard minimal; do
    "$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE "$profile" >/dev/null
    if "$ROOT/kafka-admin.sh" cluster init >/dev/null 2>&1; then
        echo "$profile unexpectedly accepted a 20 GB disk" >&2
        exit 1
    fi
    assert_eq "$(sha256sum "$TMP/cluster/manifest.json")" "$before_rejection"
done
"$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE test >/dev/null
"$ROOT/kafka-admin.sh" config set EXPECTED_DATA_DISK_GB 19 >/dev/null
if "$ROOT/kafka-admin.sh" config validate >/dev/null 2>&1; then
    echo "test unexpectedly accepted a disk smaller than 20 GB" >&2
    exit 1
fi
"$ROOT/kafka-admin.sh" config set EXPECTED_DATA_DISK_GB 20 >/dev/null

# Authentication requires a password and a dedicated client port.
"$ROOT/kafka-admin.sh" config set ENABLE_AUTH true >/dev/null
if "$ROOT/kafka-admin.sh" config validate >/dev/null 2>&1; then
    echo "authentication unexpectedly accepted an empty password" >&2; exit 1
fi
"$ROOT/kafka-admin.sh" config set AUTH_PASSWORD 'LocalTest-Password_2026' >/dev/null
"$ROOT/kafka-admin.sh" config set EXTERNAL_PORT 9092 >/dev/null
if "$ROOT/kafka-admin.sh" cluster init >/dev/null 2>&1; then
    echo "authentication unexpectedly accepted an internal port conflict" >&2; exit 1
fi
"$ROOT/kafka-admin.sh" config set EXTERNAL_PORT 19092 >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
assert_eq "$(manifest_identity)" "$first_identity"
if "$ROOT/kafka-admin.sh" config show | grep -q 'LocalTest-Password_2026'; then
    echo "config show exposed password" >&2; exit 1
fi
python3 - "$TMP/cluster" <<'AUTH_PY'
import base64, hashlib, json, pathlib, re, stat, sys
root = pathlib.Path(sys.argv[1])
security = json.loads((root / 'manifest.json').read_text())['security']
assert security == dict(authenticationEnabled=True, clientPort=19092, clientProtocol='SASL_PLAINTEXT', aclEnabled=False)
credential = (root / 'bootstrap.scram').read_text()
salt = base64.b64decode(re.search(r'salt="([^"]+)"', credential)[1])
salted = base64.b64decode(re.search(r'saltedpassword="([^"]+)"', credential)[1])
assert salted == hashlib.pbkdf2_hmac('sha512', b'LocalTest-Password_2026', salt, 8192)
for node in (root / 'nodes').iterdir():
    props = (node / 'config/server.properties').read_text()
    assert 'authorizer.class.name' not in props
    assert 'LocalTest-Password_2026' not in ''.join(p.read_text() for p in node.rglob('*') if p.is_file())
    if node.name.startswith('broker-'):
        assert 'EXTERNAL://0.0.0.0:19092' in props
        assert 'EXTERNAL:SASL_PLAINTEXT' in props
        assert 'inter.broker.listener.name=INTERNAL' in props
        assert 'INTERNAL:PLAINTEXT' in props and 'CONTROLLER:PLAINTEXT' in props
        assert 'listener.name.external.sasl.enabled.mechanisms=SCRAM-SHA-512' in props
        assert not (node / 'config/bootstrap.scram').exists()
    else:
        assert (node / 'config/bootstrap.scram').read_text() == credential
        assert 'SASL' not in props
    assert stat.S_IMODE((node / 'config/server.properties').stat().st_mode) == 0o600
client = root / 'clients/admin.properties'
assert stat.S_IMODE(client.stat().st_mode) == 0o600
assert 'security.protocol=SASL_PLAINTEXT' in client.read_text()
assert '10.20.0.13:19092' in client.read_text()
AUTH_PY
first_credential="$(sha256sum "$TMP/cluster/bootstrap.scram")"
"$ROOT/kafka-admin.sh" cluster init >/dev/null
assert_eq "$(sha256sum "$TMP/cluster/bootstrap.scram")" "$first_credential"
# Reject accidental bootstrap credential changes and preserve generated packages.
"$ROOT/kafka-admin.sh" config set AUTH_PASSWORD 'Different-Password_2026' >/dev/null
if "$ROOT/kafka-admin.sh" cluster init >/dev/null 2>&1; then
    echo "changed bootstrap credentials unexpectedly accepted" >&2; exit 1
fi
assert_eq "$(sha256sum "$TMP/cluster/bootstrap.scram")" "$first_credential"
"$ROOT/kafka-admin.sh" config set AUTH_PASSWORD 'LocalTest-Password_2026' >/dev/null
# Credential integrity is part of node package validation.
printf '\n' >> "$TMP/cluster/nodes/controller-1-10.20.0.11/config/bootstrap.scram"
if "$ROOT/kafka-admin.sh" cluster validate >/dev/null 2>&1; then
    echo "tampered SCRAM credential unexpectedly passed" >&2; exit 1
fi
# Disabling auth removes generated secrets and restores PLAINTEXT.
"$ROOT/kafka-admin.sh" config set ENABLE_AUTH false >/dev/null
"$ROOT/kafka-admin.sh" config set ENABLE_EXTERNAL_LISTENER true >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
[[ ! -e "$TMP/cluster/bootstrap.scram" && ! -e "$TMP/cluster/clients" ]]
grep -q 'EXTERNAL:PLAINTEXT' "$TMP/cluster/nodes/broker-1-10.20.0.11/config/server.properties"
# Older config files default to no authentication.
sed -i.bak '/^ENABLE_AUTH=/d; /^AUTH_USERNAME=/d; /^AUTH_PASSWORD=/d' "$KAFKA_CONFIG_FILE"
"$ROOT/kafka-admin.sh" config validate >/dev/null

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
