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

# Manual limits propagate to all six roles without changing cluster identities.
"$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE manual >/dev/null
if "$ROOT/kafka-admin.sh" config validate >/dev/null 2>&1; then
    echo "manual accepted missing limits" >&2; exit 1
fi
"$ROOT/kafka-admin.sh" config set MANUAL_CONTROLLER_CPU 0.75 >/dev/null
"$ROOT/kafka-admin.sh" config set MANUAL_CONTROLLER_MEMORY 768m >/dev/null
"$ROOT/kafka-admin.sh" config set MANUAL_BROKER_CPU 1.25 >/dev/null
"$ROOT/kafka-admin.sh" config set MANUAL_BROKER_MEMORY 3g >/dev/null
"$ROOT/kafka-admin.sh" cluster init >/dev/null
"$ROOT/kafka-admin.sh" cluster validate >/dev/null
assert_eq "$(manifest_identity)" "$first_identity"
python3 - "$TMP/cluster" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / "manifest.json").read_text())
assert manifest["profile"] == "manual"
assert "diskGB" not in manifest["storage"]
assert len(manifest["controllers"]) == len(manifest["brokers"]) == 3
nodes = list((root / "nodes").iterdir())
assert len(nodes) == 6
for node in nodes:
    manifest = json.loads((node / "manifest.json").read_text())
    role = manifest['node']['role']
    index = manifest['node']['index']
    leaf = 'metadata' if role == 'controller' else 'data'
    assert manifest["profile"] == "manual"
    assert manifest['paths']['deploy'] == f'/root/deploy/kafka/{role}'
    assert manifest['paths']['data'] == f'/data1/kafka/{role}/{leaf}'
    env = (node / '.env').read_text()
    assert f'DEPLOY_DIR=/root/deploy/kafka/{role}\n' in env
    assert f'HOST_DATA_DIR=/data1/kafka/{role}/{leaf}\n' in env
    assert f'NODE_ID={100+index if role == "controller" else index}\n' in env
    assert 'EXPECTED_DATA_DISK_GB=' not in (node / '.env').read_text()
    props = (node / "config/server.properties").read_text()
    if node.name.startswith("broker-"):
        for setting in ("default.replication.factor=3", "min.insync.replicas=2",
                        "offsets.topic.replication.factor=3", "transaction.state.log.replication.factor=3",
                        "transaction.state.log.min.isr=2", "auto.create.topics.enable=false"):
            assert setting in props.splitlines(), (node, setting)
assert json.loads((root / "manifest.json").read_text())["resources"] == {
    "controller": {"cpu": "0.75", "memoryMiB": 768}, "broker": {"cpu": "1.25", "memoryMiB": 3072}
}
readme = (root / 'README.md').read_text()
cluster_manifest = json.loads((root / 'manifest.json').read_text())
assert cluster_manifest['cluster']['id'] not in readme
assert cluster_manifest['cluster']['name'] not in readme
for node in cluster_manifest['controllers'] + cluster_manifest['brokers']:
    assert node['host'] not in readme
PY
for role in controller broker; do
    for index in 1 2 3; do
        source "$TMP/cluster/nodes/$role-$index-10.20.0.$((10+index))/.env"
        assert_eq "$DEPLOY_PROFILE" "manual"
        if [[ "$role" == controller ]]; then
            assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "0.75/768m/-Xms384m -Xmx384m"
        else
            assert_eq "$CPU_LIMIT/$MEMORY_LIMIT/$KAFKA_HEAP_OPTS" "1.25/3g/-Xms1024m -Xmx1024m"
        fi
    done
done

# All profiles initialize without a fixed disk-size setting.
for profile in standard minimal; do
    "$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE "$profile" >/dev/null
    "$ROOT/kafka-admin.sh" cluster init >/dev/null
    "$ROOT/kafka-admin.sh" cluster validate >/dev/null
    assert_eq "$(manifest_identity)" "$first_identity"
done
"$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE manual >/dev/null
"$ROOT/kafka-admin.sh" config validate >/dev/null

# Kafka storage is configurable without a Docker storage setting.
(
    export KAFKA_CONFIG_FILE="$TMP/custom-paths.conf"
    export KAFKA_CLUSTER_DIR="$TMP/custom-paths-cluster"
    cp "$TMP/kafka-admin.conf" "$KAFKA_CONFIG_FILE"
    "$ROOT/kafka-admin.sh" config set KAFKA_DATA_BASE_DIR /srv/kafka-volume/messages >/dev/null
    "$ROOT/kafka-admin.sh" cluster init >/dev/null
    "$ROOT/kafka-admin.sh" cluster validate >/dev/null
    python3 - "$KAFKA_CLUSTER_DIR" <<'PATH_PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
paths = json.loads((root / 'manifest.json').read_text())['paths']
assert paths['dataBaseDir'] == '/srv/kafka-volume/messages'
assert 'dataMountPoint' not in paths
assert 'dockerDataRoot' not in paths
for node in (root / 'nodes').iterdir():
    manifest = json.loads((node / 'manifest.json').read_text())
    role = manifest['node']['role']
    leaf = 'metadata' if role == 'controller' else 'data'
    assert manifest['paths']['data'] == f'/srv/kafka-volume/messages/{role}/{leaf}'
    assert manifest['paths']['deploy'] == f'/root/deploy/kafka/{role}'
    env = (node / '.env').read_text()
    assert 'KAFKA_DATA_BASE_DIR=/srv/kafka-volume/messages' in env
    assert 'DATA_MOUNT_POINT=' not in env
    assert 'DOCKER_DATA_ROOT=' not in env
PATH_PY
    for invalid in '/' relative '/srv/kafka-volume/../escape' '/srv/kafka-volume/a b' '/srv/kafka-volume/a;id' '/srv/kafka-volume/a:ro'; do
        "$ROOT/kafka-admin.sh" config set KAFKA_DATA_BASE_DIR "$invalid" >/dev/null
        if "$ROOT/kafka-admin.sh" config validate >/dev/null 2>&1; then
            echo 'invalid storage path unexpectedly accepted' >&2; exit 1
        fi
    done

)

# Exercise host checks without Docker or touching real data disks.
python3 - "$ROOT/templates/deploy.sh.tmpl" "$TMP" <<'DISK_PY'
import os, pathlib, subprocess, sys
template = pathlib.Path(sys.argv[1]).read_text().split('\ncase "${1:-}" in', 1)[0]
probe = pathlib.Path(sys.argv[2]) / 'disk-check.sh'
probe.write_text(template + r'''
SCRIPT_DIR=/root/deploy/kafka/controller
DEPLOY_DIR=$SCRIPT_DIR
NODE_ROLE=controller
NODE_INDEX=1
KAFKA_DATA_BASE_DIR=$MOCK_DATA_BASE
HOST_DATA_DIR=$KAFKA_DATA_BASE_DIR/controller/metadata
# Host storage checks must not inspect Docker's data-root.
docker() { echo 'unexpected Docker storage query' >&2; return 1; }
findmnt() {
    [[ "$5" == "$MOCK_EXPECTED_PROBE" ]] || return 1
    [[ "$MOCK_FINDMNT_FAIL" == false ]] || return 1
    if [[ "$MOCK_MOUNTED" == true ]]; then
        printf '%s\n' "$MOCK_DATA_MOUNT"
    else
        printf '/\n'
    fi
}
df() {
    if [[ "$1" == -BG ]]; then
        [[ "$3" == "$MOCK_EXPECTED_PROBE" ]] || return 1
        printf 'Avail\n%sG\n' "$MOCK_AVAILABLE_GB"
        return
    fi
    [[ "$1" == -P ]] || { echo 'unexpected total-capacity check' >&2; return 1; }
    [[ "$2" == "$MOCK_EXPECTED_PROBE" ]] || return 1
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted\n'
    printf '/dev/test 5000000 3000000 2000000 %s%% /data1\n' "$MOCK_USED_PCT"
}
check_host
''')
base = pathlib.Path(sys.argv[2]).resolve()
def check(profile, used=60, mounted='true',
          data_mount='/data1', data_base=None, available=5,
          findmnt_fail='false', expected_probe=None):
    env = dict(os.environ, DEPLOY_PROFILE=profile,
               MOCK_USED_PCT=str(used), MOCK_MOUNTED=mounted,
               MOCK_DATA_BASE=str(data_base or base/'missing'/'messages'), MOCK_DATA_MOUNT=data_mount,
               MOCK_EXPECTED_PROBE=str(expected_probe or base), MOCK_FINDMNT_FAIL=findmnt_fail,
               MOCK_AVAILABLE_GB=str(available))
    return subprocess.run(['bash', str(probe)], env=env, text=True, capture_output=True)
for profile in ('standard', 'minimal'):
    result = check(profile)
    assert result.returncode == 0, result.stderr
    result = check(profile, used=70)
    assert result.returncode == 0 and '规划线' in result.stderr, result.stderr
    result = check(profile, used=85)
    assert result.returncode != 0 and '阻断阈值' in result.stderr, result.stderr
    result = check(profile, mounted='false')
    assert result.returncode != 0 and '系统根文件系统' in result.stderr, result.stderr
    custom = dict(data_mount='/srv/kafka-volume', data_base=base/'custom'/'messages')
    result = check(profile, **custom)
    assert result.returncode == 0, result.stderr
    result = check(profile, mounted='false', **custom)
    assert result.returncode != 0 and '系统根文件系统' in result.stderr, result.stderr
    result = check(profile, used=85, **custom)
    assert result.returncode != 0 and '/srv/kafka-volume' in result.stderr, result.stderr
    result = check(profile, findmnt_fail='true')
    assert result.returncode != 0 and '无法识别' in result.stderr, result.stderr
# Existing role directories may sit on a more specific filesystem than the base.
role = base/'nested'/'controller'/'metadata'
role.mkdir(parents=True)
result = check('minimal', data_base=base/'nested', expected_probe=role, data_mount='/nested-mount')
assert result.returncode == 0 and '/nested-mount' in result.stdout, result.stderr
# Dangling links must not be mistaken for a missing directory that prepare can create.
link = base/'dangling'
link.symlink_to(base/'missing-target')
result = check('minimal', data_base=link)
assert result.returncode != 0 and '失效的符号链接' in result.stderr, result.stderr
result = check('manual', mounted='false')
assert result.returncode == 0, result.stderr
result = check('manual', available=4)
assert result.returncode != 0 and '可用空间不足' in result.stderr, result.stderr
DISK_PY

# A new authenticated cluster can generate and persist its initial password.
(
    export KAFKA_CONFIG_FILE="$TMP/random-password.conf"
    export KAFKA_CLUSTER_DIR="$TMP/random-password-cluster"
    cp "$ROOT/config/kafka-admin.conf.example" "$KAFKA_CONFIG_FILE"
    "$ROOT/kafka-admin.sh" config set CONTROLLER_HOSTS '10.20.0.11,10.20.0.12,10.20.0.13' >/dev/null
    "$ROOT/kafka-admin.sh" config set BROKER_HOSTS '10.20.0.11,10.20.0.12,10.20.0.13' >/dev/null
    "$ROOT/kafka-admin.sh" config set ENABLE_AUTH true >/dev/null
    "$ROOT/kafka-admin.sh" cluster init > "$TMP/random-password.log" 2>&1
    "$ROOT/kafka-admin.sh" cluster validate >/dev/null
    source "$KAFKA_CONFIG_FILE"
    [[ "$AUTH_PASSWORD" =~ ^[a-zA-Z0-9]{6}$ ]] || { echo 'invalid generated password' >&2; exit 1; }
    saved_password="$AUTH_PASSWORD"
    saved_credential="$(sha256sum "$KAFKA_CLUSTER_DIR/bootstrap.scram")"
    "$ROOT/kafka-admin.sh" cluster init >> "$TMP/random-password.log" 2>&1
    source "$KAFKA_CONFIG_FILE"
    [[ "$AUTH_PASSWORD" == "$saved_password" ]] || { echo 'generated password changed' >&2; exit 1; }
    assert_eq "$(sha256sum "$KAFKA_CLUSTER_DIR/bootstrap.scram")" "$saved_credential"
    AUTH_PASSWORD="$AUTH_PASSWORD" python3 - "$KAFKA_CONFIG_FILE" "$KAFKA_CLUSTER_DIR" "$TMP/random-password.log" <<'RANDOM_PY'
import os, pathlib, stat, sys
config, cluster, logs = map(pathlib.Path, sys.argv[1:])
password = os.environ['AUTH_PASSWORD']
assert password not in logs.read_text(), 'generated password exposed in log'
assert stat.S_IMODE(config.stat().st_mode) == 0o600
assert stat.S_IMODE((cluster / 'clients/admin.properties').stat().st_mode) == 0o600
assert f'password="{password}"' in (cluster / 'clients/admin.properties').read_text()
RANDOM_PY
    # Missing local credentials must not silently rotate an existing cluster.
    printf '\nAUTH_PASSWORD=""\n' >> "$KAFKA_CONFIG_FILE"
    if "$ROOT/kafka-admin.sh" cluster init >/dev/null 2>&1; then
        echo 'missing existing password unexpectedly regenerated' >&2; exit 1
    fi
    source "$KAFKA_CONFIG_FILE"
    [[ -z "$AUTH_PASSWORD" ]] || { echo 'missing password overwritten' >&2; exit 1; }
    assert_eq "$(sha256sum "$KAFKA_CLUSTER_DIR/bootstrap.scram")" "$saved_credential"

    # Explicit passwords can be as short as one character.
    export KAFKA_CLUSTER_DIR="$TMP/manual-password-cluster"
    "$ROOT/kafka-admin.sh" config set AUTH_PASSWORD x >/dev/null
    "$ROOT/kafka-admin.sh" config validate >/dev/null
    "$ROOT/kafka-admin.sh" cluster init >/dev/null
    "$ROOT/kafka-admin.sh" cluster validate >/dev/null
    python3 - "$KAFKA_CLUSTER_DIR" <<'SHORT_PY'
import base64, hashlib, pathlib, re, sys
root = pathlib.Path(sys.argv[1])
credential = (root / 'bootstrap.scram').read_text()
salt = base64.b64decode(re.search(r'salt="([^"]+)"', credential)[1])
salted = base64.b64decode(re.search(r'saltedpassword="([^"]+)"', credential)[1])
assert salted == hashlib.pbkdf2_hmac('sha512', b'x', salt, 8192)
assert 'password="x"' in (root / 'clients/admin.properties').read_text()
SHORT_PY
)

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

python3 "$ROOT/tests/test-controller-health.py"
echo "Kafka basic tests passed"
