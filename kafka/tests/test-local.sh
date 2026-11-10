#!/usr/bin/env bash
# Three isolated Linux nodes with independent Docker daemons; not three independent VMs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="$ROOT/generated/local-test"
mkdir -p "$RUNTIME/logs"
export KAFKA_CONFIG_FILE="$RUNTIME/kafka-admin.conf"
export KAFKA_CLUSTER_DIR="$RUNTIME/cluster"
export KAFKA_INTERACTIVE=false

nodes() { docker compose -f "$ROOT/tests/local/docker-compose.yml" "$@"; }
node() { local index="$1"; shift; docker exec -i "hr-kafka-local-test-node-$index-1" "$@"; }
deploy() { local index="$1" role="$2"; shift 2; node "$index" bash "/root/deploy/kafka/$role-$index/deploy.sh" "$@"; }

generate() {
    [[ -f "$KAFKA_CONFIG_FILE" ]] || cp "$ROOT/config/kafka-admin.conf.example" "$KAFKA_CONFIG_FILE"
    chmod 600 "$KAFKA_CONFIG_FILE"
    "$ROOT/kafka-admin.sh" config set CLUSTER_NAME hr-kafka-local-test
    "$ROOT/kafka-admin.sh" config set DEPLOY_PROFILE test
    "$ROOT/kafka-admin.sh" config set EXPECTED_DATA_DISK_GB 20
    "$ROOT/kafka-admin.sh" config set CONTROLLER_HOSTS 172.30.93.11,172.30.93.12,172.30.93.13
    "$ROOT/kafka-admin.sh" config set BROKER_HOSTS 172.30.93.11,172.30.93.12,172.30.93.13
    "$ROOT/kafka-admin.sh" cluster init
    "$ROOT/kafka-admin.sh" cluster validate
}

distribute() {
    local index role
    for index in 1 2 3; do
        node "$index" mkdir -p /root/deploy/kafka
        for role in controller broker; do
            node "$index" mkdir -p "/root/deploy/kafka/$role-$index"
            docker cp "$KAFKA_CLUSTER_DIR/nodes/$role-$index-172.30.93.$((10+index))/." \
                "hr-kafka-local-test-node-$index-1:/root/deploy/kafka/$role-$index/"
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
        generate
        docker image inspect apache/kafka:4.3.2 >/dev/null 2>&1 || docker pull apache/kafka:4.3.2
        nodes up -d --build
        for index in 1 2 3; do
            ready=false
            for ((attempt=0; attempt<60; attempt++)); do
                if node "$index" docker info >/dev/null 2>&1; then ready=true; break; fi
                sleep 2
            done
            [[ "$ready" == true ]] || { nodes logs --tail 60; exit 1; }
            # Import the same cached image into each daemon, avoiding registry/tag differences.
            docker save apache/kafka:4.3.2 | node "$index" docker load
            node "$index" docker info --format 'root={{.DockerRootDir}} architecture={{.Architecture}}'
            node "$index" df -h /data1
        done
        distribute
        ;;
    prepare)
        generate
        distribute
        for role in controller broker; do
            for index in 1 2 3; do deploy "$index" "$role" check; deploy "$index" "$role" prepare; done
        done
        ;;
    start)
        for role in controller broker; do
            for index in 1 2 3; do deploy "$index" "$role" start; done
        done
        ;;
    verify) python3 "$ROOT/tests/local/verify.py" "$RUNTIME" ;;
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
