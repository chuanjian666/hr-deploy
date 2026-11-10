#!/usr/bin/env bash

[[ -n "${_KAFKA_CLUSTER_ENGINE_SH_LOADED:-}" ]] && return 0
readonly _KAFKA_CLUSTER_ENGINE_SH_LOADED=1

_ENGINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_ENGINE_DIR/../lib/common.sh"
source "$_ENGINE_DIR/../lib/config.sh"
source "$_ENGINE_DIR/../lib/validation.sh"

cluster_dir() { printf '%s\n' "${KAFKA_CLUSTER_DIR:-$(script_root)/cluster}"; }

new_kafka_id() {
    openssl rand 16 | base64 | tr '+/' '-_' | tr -d '=\n'
}

read_existing_identities() {
    local manifest="$(cluster_dir)/manifest.json"
    CLUSTER_ID=""
    CONTROLLER_DIRECTORY_IDS=()
    [[ -f "$manifest" ]] || return 0
    require_command python3
    local identity_lines=() line
    while IFS= read -r line; do identity_lines+=("$line"); done < <(python3 - "$manifest" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    data=json.load(f)
print(data["cluster"]["id"])
for node in data["controllers"]:
    print(node["directoryId"])
PY
)
    [[ ${#identity_lines[@]} -eq 4 ]] || die "现有 manifest.json 身份信息不完整"
    CLUSTER_ID="${identity_lines[0]}"
    CONTROLLER_DIRECTORY_IDS=("${identity_lines[@]:1}")
}

interactive_config() {
    [[ -t 0 && "${KAFKA_INTERACTIVE:-true}" == true ]] || return 0
    local input
    echo "=== Kafka 集群初始化 ==="
    read -r -p "集群名称 [$CLUSTER_NAME]: " input; [[ -z "$input" ]] || save_config CLUSTER_NAME "$input"
    read -r -p "Kafka 镜像 [$KAFKA_IMAGE]: " input; [[ -z "$input" ]] || save_config KAFKA_IMAGE "$input"
    read -r -p "部署规格 standard/minimal/test（test 仅功能验证）[$DEPLOY_PROFILE]: " input; [[ -z "$input" ]] || save_config DEPLOY_PROFILE "$input"
    read -r -p "磁盘容量 GB（生产至少 512，test 至少 20）[$EXPECTED_DATA_DISK_GB]: " input; [[ -z "$input" ]] || save_config EXPECTED_DATA_DISK_GB "$input"
    read -r -p "3 个 controller 地址，逗号分隔 [$CONTROLLER_HOSTS]: " input; [[ -z "$input" ]] || save_config CONTROLLER_HOSTS "$input"
    read -r -p "3 个 broker 地址，逗号分隔 [$BROKER_HOSTS]: " input; [[ -z "$input" ]] || save_config BROKER_HOSTS "$input"
    load_config
}

profile_values() {
    case "$DEPLOY_PROFILE" in
        standard)
            CONTROLLER_CPU=1; CONTROLLER_MEMORY=2g; CONTROLLER_HEAP=1g
            BROKER_CPU=6; BROKER_MEMORY=24g; BROKER_HEAP=8g
            ;;
        minimal)
            CONTROLLER_CPU=1; CONTROLLER_MEMORY=2g; CONTROLLER_HEAP=1g
            BROKER_CPU=2; BROKER_MEMORY=10g; BROKER_HEAP=4g
            ;;
        test)
            CONTROLLER_CPU=0.5; CONTROLLER_MEMORY=512m; CONTROLLER_HEAP=256m
            BROKER_CPU=1; BROKER_MEMORY=1536m; BROKER_HEAP=512m
            ;;
    esac
}

render_template() {
    local source="$1" target="$2"; shift 2
    require_command python3
    python3 - "$source" "$target" "$@" <<'PY'
import sys
source, target, *pairs = sys.argv[1:]
text=open(source, encoding="utf-8").read()
for pair in pairs:
    key, value=pair.split("=", 1)
    text=text.replace("{{"+key+"}}", value)
if "{{" in text or "}}" in text:
    raise SystemExit(f"模板仍有未替换变量: {source}")
with open(target, "w", encoding="utf-8") as f:
    f.write(text)
PY
}

write_env() {
    local dir="$1" role="$2" index="$3" node_id="$4" directory_id="$5"
    local cpu memory heap data_dir container project deploy_dir
    if [[ "$role" == controller ]]; then
        cpu="$CONTROLLER_CPU"; memory="$CONTROLLER_MEMORY"; heap="$CONTROLLER_HEAP"
    else
        cpu="$BROKER_CPU"; memory="$BROKER_MEMORY"; heap="$BROKER_HEAP"
    fi
    data_dir="$DATA_ROOT/${role}-${index}/$([[ "$role" == controller ]] && echo metadata || echo data)"
    deploy_dir="$TARGET_DEPLOY_ROOT/${role}-${index}"
    container="${CLUSTER_NAME}-${role}-${index}"
    project="$container"
    cat > "$dir/.env" <<EOF
KAFKA_IMAGE=$KAFKA_IMAGE
COMPOSE_PROJECT_NAME=$project
CONTAINER_NAME=$container
NODE_ROLE=$role
DEPLOY_PROFILE=$DEPLOY_PROFILE
NODE_INDEX=$index
NODE_ID=$node_id
DIRECTORY_ID=$directory_id
CLUSTER_ID=$CLUSTER_ID
INITIAL_CONTROLLERS=$INITIAL_CONTROLLERS
DEPLOY_DIR=$deploy_dir
HOST_DATA_DIR=$data_dir
DOCKER_DATA_ROOT=$DOCKER_DATA_ROOT
EXPECTED_DATA_DISK_GB=$EXPECTED_DATA_DISK_GB
CPU_LIMIT=$cpu
MEMORY_LIMIT=$memory
KAFKA_HEAP_OPTS="-Xms$heap -Xmx$heap"
NOFILE_LIMIT=$NOFILE_LIMIT
LOG_MAX_SIZE=$LOG_MAX_SIZE
LOG_MAX_FILE=$LOG_MAX_FILE
EOF
    chmod 600 "$dir/.env"
}

write_node_manifest() {
    local dir="$1" role="$2" index="$3" node_id="$4" directory_id="$5" host="$6" data_dir="$7"
    local props_sha compose_sha env_sha log_sha deploy_sha
    props_sha="$(sha256_file "$dir/config/server.properties")"
    compose_sha="$(sha256_file "$dir/docker-compose.yml")"
    env_sha="$(sha256_file "$dir/.env")"
    log_sha="$(sha256_file "$dir/config/log4j2.yaml")"
    deploy_sha="$(sha256_file "$dir/deploy.sh")"
    cat > "$dir/checksums.sha256" <<EOF
$props_sha  config/server.properties
$compose_sha  docker-compose.yml
$env_sha  .env
$log_sha  config/log4j2.yaml
$deploy_sha  deploy.sh
EOF
    python3 - "$dir/manifest.json" <<PY
import json
manifest={
  "manifestVersion": 1,
  "cluster": {"name": "$(json_escape "$CLUSTER_NAME")", "id": "$(json_escape "$CLUSTER_ID")"},
  "node": {"role": "$role", "index": $index, "nodeId": $node_id,
           "directoryId": "$(json_escape "$directory_id")", "host": "$(json_escape "$host")"},
  "kafka": {"image": "$(json_escape "$KAFKA_IMAGE")"},
  "profile": "$DEPLOY_PROFILE",
  "paths": {"deploy": "$(json_escape "$TARGET_DEPLOY_ROOT/${role}-${index}")", "data": "$(json_escape "$data_dir")"},
  "checksums": {"serverProperties": "$props_sha", "compose": "$compose_sha", "env": "$env_sha", "log4j2": "$log_sha", "deploy": "$deploy_sha"}
}
with open("$dir/manifest.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False); f.write("\n")
PY
    chmod 600 "$dir/manifest.json" "$dir/checksums.sha256"
}

generate_node() {
    local nodes_dir="$1" role="$2" index="$3" host="$4" node_id directory_id="" dir listeners advertised data_dir
    if [[ "$role" == controller ]]; then
        node_id=$((100 + index)); directory_id="${CONTROLLER_DIRECTORY_IDS[$((index-1))]}"
    else
        node_id="$index"
    fi
    dir="$nodes_dir/${role}-${index}-${host}"
    mkdir -p "$dir/config"
    cp "$(script_root)/templates/docker-compose.yml.tmpl" "$dir/docker-compose.yml"
    cp "$(script_root)/templates/log4j2.yaml.tmpl" "$dir/config/log4j2.yaml"
    render_template "$(script_root)/templates/deploy.sh.tmpl" "$dir/deploy.sh" \
        "NODE_ROLE=$role" "NODE_INDEX=$index"
    chmod 700 "$dir/deploy.sh"

    if [[ "$role" == controller ]]; then
        render_template "$(script_root)/templates/controller.properties.tmpl" "$dir/config/server.properties" \
            "NODE_ID=$node_id" "CONTROLLER_BOOTSTRAP_SERVERS=$CONTROLLER_BOOTSTRAP_SERVERS" \
            "CONTROLLER_PORT=$CONTROLLER_PORT"
        data_dir="$DATA_ROOT/controller-${index}/metadata"
    else
        listeners="INTERNAL://0.0.0.0:${BROKER_PORT}"
        advertised="INTERNAL://${host}:${BROKER_PORT}"
        if [[ "$ENABLE_EXTERNAL_LISTENER" == true ]]; then
            listeners+=",EXTERNAL://0.0.0.0:${EXTERNAL_PORT}"
            advertised+=",EXTERNAL://${host}:${EXTERNAL_PORT}"
        fi
        render_template "$(script_root)/templates/broker.properties.tmpl" "$dir/config/server.properties" \
            "NODE_ID=$node_id" "CONTROLLER_BOOTSTRAP_SERVERS=$CONTROLLER_BOOTSTRAP_SERVERS" \
            "LISTENERS=$listeners" "ADVERTISED_LISTENERS=$advertised" \
            "DELETE_TOPIC_ENABLE=$DELETE_TOPIC_ENABLE" "LOG_RETENTION_HOURS=$LOG_RETENTION_HOURS" \
            "LOG_SEGMENT_BYTES=$LOG_SEGMENT_BYTES" "LOG_ROLL_HOURS=$LOG_ROLL_HOURS" \
            "LOG_RETENTION_CHECK_INTERVAL_MS=$LOG_RETENTION_CHECK_INTERVAL_MS"
        data_dir="$DATA_ROOT/broker-${index}/data"
    fi
    chmod 600 "$dir/config/server.properties" "$dir/config/log4j2.yaml"
    write_env "$dir" "$role" "$index" "$node_id" "$directory_id"
    write_node_manifest "$dir" "$role" "$index" "$node_id" "$directory_id" "$host" "$data_dir"
}

write_cluster_manifest() {
    local output="$1" generated_at
    generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    python3 - "$output/manifest.json" <<PY
import json
controllers=[]
brokers=[]
controller_hosts="$(json_escape "$CONTROLLER_HOSTS")".split(",")
broker_hosts="$(json_escape "$BROKER_HOSTS")".split(",")
directory_ids="$(IFS=,; echo "${CONTROLLER_DIRECTORY_IDS[*]}")".split(",")
for i, host in enumerate(controller_hosts, 1):
    controllers.append({"index": i, "nodeId": 100+i, "directoryId": directory_ids[i-1], "host": host, "port": int("$CONTROLLER_PORT")})
for i, host in enumerate(broker_hosts, 1):
    brokers.append({"index": i, "nodeId": i, "host": host, "port": int("$BROKER_PORT")})
manifest={
  "manifestVersion": 1,
  "generatedAt": "$generated_at",
  "cluster": {"name": "$(json_escape "$CLUSTER_NAME")", "id": "$(json_escape "$CLUSTER_ID")"},
  "kafka": {"image": "$(json_escape "$KAFKA_IMAGE")"},
  "profile": "$DEPLOY_PROFILE",
  "paths": {"deployRoot": "$TARGET_DEPLOY_ROOT", "dataRoot": "$DATA_ROOT", "dockerDataRoot": "$DOCKER_DATA_ROOT"},
  "storage": {"diskGB": int("$EXPECTED_DATA_DISK_GB"), "retentionHours": int("$LOG_RETENTION_HOURS")},
  "controllers": controllers,
  "brokers": brokers
}
with open("$output/manifest.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False); f.write("\n")
PY
    chmod 600 "$output/manifest.json"
}

write_generated_readme() {
    local output="$1"
    cat > "$output/README.md" <<EOF
# $CLUSTER_NAME 节点包

集群 ID：\`$CLUSTER_ID\`

部署规格：\`$DEPLOY_PROFILE\`

Kafka 镜像：\`$KAFKA_IMAGE\`

## 操作顺序

1. 将 nodes 下各角色目录复制到对应目标机的 \`$TARGET_DEPLOY_ROOT/<role>-<n>/\`。
2. 所有节点执行 \`./deploy.sh check\`，再执行 \`./deploy.sh prepare\`。
3. 启动 controller-1、controller-2，形成多数派后启动 controller-3。
4. 依次启动 broker-1、broker-2、broker-3。

部署包生成不代表目标节点检查或部署已完成。
EOF
    if [[ "$DEPLOY_PROFILE" == test ]]; then
        cat >> "$output/README.md" <<EOF

test 规格仅用于本地功能验证，不用于生产或性能测试。
环境要求与验收步骤见源码 \`kafka/docs/local-test.md\`。
EOF
    fi
}

cluster_init() {
    load_config
    interactive_config
    validate_cluster_config
    profile_values
    require_command openssl; require_command base64; require_command sha256sum; require_command python3

    parse_hosts CONTROLLER_HOSTS "$CONTROLLER_HOSTS"; local controller_hosts=("${PARSED_HOSTS[@]}")
    parse_hosts BROKER_HOSTS "$BROKER_HOSTS"; local broker_hosts=("${PARSED_HOSTS[@]}")
    read_existing_identities
    if [[ -z "$CLUSTER_ID" ]]; then
        CLUSTER_ID="$(new_kafka_id)"
        CONTROLLER_DIRECTORY_IDS=("$(new_kafka_id)" "$(new_kafka_id)" "$(new_kafka_id)")
    else
        log_info "复用现有 cluster ID 和 controller directory ID"
    fi

    CONTROLLER_BOOTSTRAP_SERVERS=""
    INITIAL_CONTROLLERS=""
    local i
    for ((i=0; i<3; i++)); do
        CONTROLLER_BOOTSTRAP_SERVERS+="${CONTROLLER_BOOTSTRAP_SERVERS:+,}${controller_hosts[$i]}:${CONTROLLER_PORT}"
        INITIAL_CONTROLLERS+="${INITIAL_CONTROLLERS:+,}$((101+i))@${controller_hosts[$i]}:${CONTROLLER_PORT}:${CONTROLLER_DIRECTORY_IDS[$i]}"
    done

    local root output staging
    root="$(script_root)"; output="$(cluster_dir)"; ensure_dir "$(dirname "$output")"; staging="$(mktemp -d "$(dirname "$output")/.cluster.tmp.XXXXXX")"
    trap '[[ -z "${staging:-}" || ! -e "$staging" ]] || rm -rf "$staging"' EXIT
    mkdir -p "$staging/nodes"
    for ((i=1; i<=3; i++)); do
        generate_node "$staging/nodes" controller "$i" "${controller_hosts[$((i-1))]}"
        generate_node "$staging/nodes" broker "$i" "${broker_hosts[$((i-1))]}"
    done
    write_cluster_manifest "$staging"
    write_generated_readme "$staging"
    {
        echo 'role,index,node_id,host,port,package'
        for ((i=1; i<=3; i++)); do
            echo "controller,$i,$((100+i)),${controller_hosts[$((i-1))]},$CONTROLLER_PORT,controller-$i-${controller_hosts[$((i-1))]}"
            echo "broker,$i,$i,${broker_hosts[$((i-1))]},$BROKER_PORT,broker-$i-${broker_hosts[$((i-1))]}"
        done
    } > "$staging/topology.csv"
    atomic_replace_dir "$staging" "$output"
    staging=""
    trap - EXIT
    log_success "已生成 6 个角色包: $output/nodes"
    log_info "cluster ID: $CLUSTER_ID"
}

cluster_info() {
    local manifest="$(cluster_dir)/manifest.json"
    [[ -f "$manifest" ]] || die "尚未生成集群，请先执行 cluster init"
    python3 -m json.tool "$manifest"
}

cluster_verify_static() {
    local cluster="$(cluster_dir)" dir count=0
    [[ -f "$cluster/manifest.json" ]] || die "尚未生成集群"
    python3 -m json.tool "$cluster/manifest.json" >/dev/null
    while IFS= read -r -d '' dir; do
        (cd "$dir" && sha256sum -c checksums.sha256 >/dev/null)
        grep -q '^auto.create.topics.enable=false$' "$dir/config/server.properties" || [[ "$(basename "$dir")" == controller-* ]] || die "broker 未关闭自动创建 Topic: $dir"
        if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
            docker compose --env-file "$dir/.env" -f "$dir/docker-compose.yml" config --quiet
        fi
        count=$((count+1))
    done < <(find "$cluster/nodes" -mindepth 1 -maxdepth 1 -type d -print0)
    [[ $count -eq 6 ]] || die "节点包数量错误: $count"
    log_success "静态校验通过：6 个节点包、摘要、JSON、Kafka 关键配置和 Compose 配置均有效"
}
