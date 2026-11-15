#!/usr/bin/env bash
# Three isolated Linux nodes with independent Docker daemons; not three independent VMs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="$ROOT/generated/local-test"
mkdir -p "$RUNTIME/logs"
CLUSTER_DIR="${KAFKA_CLUSTER_DIR:-$ROOT/cluster}"

nodes() { docker compose -f "$ROOT/tests/local/docker-compose.yml" "$@"; }
node() { local index="$1"; shift; docker exec -i "hr-kafka-local-test-node-$index-1" "$@"; }
deploy() { local index="$1" role="$2"; shift 2; node "$index" bash "/root/deploy/kafka/$role/deploy.sh" "$@"; }

# Read the packages produced by cluster init without changing the active cluster.
local_image() {
    [[ -f "$CLUSTER_DIR/manifest.json" ]] || { echo "请先执行 ./kafka-admin.sh cluster init 并选择 manual 规格" >&2; return 1; }
    python3 - "$CLUSTER_DIR/manifest.json" <<'PYTHON'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    manifest = json.load(stream)
hosts = [f"172.30.93.{i}" for i in (11, 12, 13)]
if manifest["profile"] != "manual" or any(
    [node["host"] for node in manifest[role]] != hosts for role in ("controllers", "brokers")
):
    sys.exit("本地模拟节点需要 manual 规格，controller 和 broker 地址均依次为 " + ",".join(hosts))
print(manifest["kafka"]["image"])
PYTHON
}

distribute() {
    local index role
    for index in 1 2 3; do
        node "$index" mkdir -p /root/deploy/kafka
        for role in controller broker; do
            node "$index" mkdir -p "/root/deploy/kafka/$role"
            docker cp "$CLUSTER_DIR/nodes/$role-$index-172.30.93.$((10+index))/." \
                "hr-kafka-local-test-node-$index-1:/root/deploy/kafka/$role/"
        done
    done
}

command="${1:-help}"
case "$command" in
    up|prepare|start|verify|stop|status)
        exec > >(tee "$RUNTIME/logs/${command}-$(date +%Y%m%d-%H%M%S).log") 2>&1
        ;;
esac

case "$command" in
    up)
        image="$(local_image)"
        "$ROOT/kafka-admin.sh" cluster validate
        docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image"
        nodes up -d --build
        for index in 1 2 3; do
            ready=false
            for ((attempt=0; attempt<60; attempt++)); do
                if node "$index" docker info >/dev/null 2>&1; then ready=true; break; fi
                sleep 2
            done
            [[ "$ready" == true ]] || { nodes logs --tail 60; exit 1; }
            # Import the same cached image into each daemon, avoiding registry/tag differences.
            docker save "$image" | node "$index" docker load
            node "$index" docker info --format 'root={{.DockerRootDir}} architecture={{.Architecture}}'
            node "$index" df -h /data1
        done
        distribute
        ;;
    prepare)
        local_image >/dev/null
        "$ROOT/kafka-admin.sh" cluster validate
        distribute
        for role in controller broker; do
            for index in 1 2 3; do deploy "$index" "$role" check; deploy "$index" "$role" prepare; done
        done
        ;;
    start)
        for index in 1 2 3; do deploy "$index" controller start; done
        deploy 1 controller health 120
        for index in 1 2 3; do deploy "$index" broker start; done
        ;;
    verify)
        local_image >/dev/null
        python3 "$ROOT/tests/local/verify.py" "$RUNTIME" "$CLUSTER_DIR"
        ;;
    stop)
        for role in broker controller; do
            for index in 1 2 3; do deploy "$index" "$role" stop; done
        done
        nodes stop
        ;;
    status)
        nodes ps -a
        for index in 1 2 3; do node "$index" docker ps -a; done
        ;;
    *) echo "Usage: $0 {up|prepare|start|verify|status|stop}" ;;
esac
