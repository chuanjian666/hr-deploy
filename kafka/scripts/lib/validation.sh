#!/usr/bin/env bash

[[ -n "${_KAFKA_VALIDATION_SH_LOADED:-}" ]] && return 0
readonly _KAFKA_VALIDATION_SH_LOADED=1

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

validate_name() {
    [[ "$2" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$ ]] || die "$1 不是合法名称: $2"
}
validate_image() {
    [[ "$2" =~ ^[A-Za-z0-9._:/@-]+$ ]] || die "$1 不是合法镜像地址: $2"
}
validate_port() {
    [[ "$2" =~ ^[0-9]+$ ]] && (( 10#$2 >= 1 && 10#$2 <= 65535 )) || die "$1 必须是 1-65535 的整数"
}
validate_uint() { [[ "$2" =~ ^[0-9]+$ ]] || die "$1 必须是非负整数"; }
validate_boolean() { [[ "$2" == true || "$2" == false ]] || die "$1 必须是 true 或 false"; }
validate_absolute_path() {
    [[ "$2" == /* && "$2" != "/" && "$2" != *[[:space:]]* ]] || die "$1 必须是无空格的非根绝对路径: $2"
}
validate_host() {
    local name="$1" value="$2" label octet
    [[ -n "$value" && "$value" != *[[:space:]]* && "$value" != */* ]] || die "$name 不是合法主机名或 IP: $value"
    if [[ "$value" =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
        IFS=. read -r -a parts <<< "$value"
        for octet in "${parts[@]}"; do (( 10#$octet <= 255 )) || die "$name 不是合法 IPv4: $value"; done
        return
    fi
    [[ "$value" != .* && "$value" != *. && "$value" != *..* ]] || die "$name 不是合法主机名: $value"
    IFS=. read -r -a labels <<< "$value"
    for label in "${labels[@]}"; do
        [[ "$label" =~ ^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$ ]] || die "$name 不是合法主机名: $value"
    done
}

parse_hosts() {
    local name="$1" raw="$2" item
    raw="${raw// /,}"
    while [[ "$raw" == *,,* ]]; do raw="${raw//,,/,}"; done
    raw="${raw#,}"; raw="${raw%,}"
    IFS=',' read -r -a PARSED_HOSTS <<< "$raw"
    [[ ${#PARSED_HOSTS[@]} -eq 3 ]] || die "$name 必须正好包含 3 个地址"
    for item in "${PARSED_HOSTS[@]}"; do validate_host "$name" "$item"; done
}

validate_cluster_config() {
    validate_name CLUSTER_NAME "$CLUSTER_NAME"
    validate_image KAFKA_IMAGE "$KAFKA_IMAGE"
    [[ "$DEPLOY_PROFILE" == standard || "$DEPLOY_PROFILE" == minimal || "$DEPLOY_PROFILE" == test ]] || die "DEPLOY_PROFILE 必须是 standard、minimal 或 test"
    parse_hosts CONTROLLER_HOSTS "$CONTROLLER_HOSTS"; local controllers=("${PARSED_HOSTS[@]}")
    parse_hosts BROKER_HOSTS "$BROKER_HOSTS"; local brokers=("${PARSED_HOSTS[@]}")
    validate_port CONTROLLER_PORT "$CONTROLLER_PORT"
    validate_port BROKER_PORT "$BROKER_PORT"
    validate_boolean ENABLE_EXTERNAL_LISTENER "$ENABLE_EXTERNAL_LISTENER"
    validate_port EXTERNAL_PORT "$EXTERNAL_PORT"
    [[ "$CONTROLLER_PORT" != "$BROKER_PORT" ]] || die "CONTROLLER_PORT 和 BROKER_PORT 不能相同"
    validate_boolean ENABLE_AUTH "$ENABLE_AUTH"
    if [[ "$ENABLE_AUTH" == true ]]; then
        validate_name AUTH_USERNAME "$AUTH_USERNAME"
        [[ ${#AUTH_PASSWORD} -ge 16 && "$AUTH_PASSWORD" =~ ^[a-zA-Z0-9_@%+=:./-]+$ ]] || die "AUTH_PASSWORD 必须至少 16 位，仅支持 ASCII 字母、数字或 _@%+=:./-"
    fi
    if [[ "$ENABLE_EXTERNAL_LISTENER" == true || "$ENABLE_AUTH" == true ]]; then
        [[ "$EXTERNAL_PORT" != "$CONTROLLER_PORT" && "$EXTERNAL_PORT" != "$BROKER_PORT" ]] || die "EXTERNAL_PORT 不能与其他端口相同"
    fi
    validate_absolute_path TARGET_DEPLOY_ROOT "$TARGET_DEPLOY_ROOT"
    validate_absolute_path DATA_ROOT "$DATA_ROOT"
    validate_absolute_path DOCKER_DATA_ROOT "$DOCKER_DATA_ROOT"
    [[ "$TARGET_DEPLOY_ROOT" == /root/deploy/kafka ]] || die "TARGET_DEPLOY_ROOT 当前固定为 /root/deploy/kafka"
    [[ "$DATA_ROOT" == /data1/kafka ]] || die "DATA_ROOT 当前固定为 /data1/kafka"
    [[ "$DOCKER_DATA_ROOT" == /data1/docker ]] || die "DOCKER_DATA_ROOT 当前固定为 /data1/docker"
    validate_uint EXPECTED_DATA_DISK_GB "$EXPECTED_DATA_DISK_GB"
    if [[ "$DEPLOY_PROFILE" == test ]]; then
        (( EXPECTED_DATA_DISK_GB >= 20 )) || die "test 规格 EXPECTED_DATA_DISK_GB 不能小于 20"
        log_warn "test 规格仅用于本地 3 节点部署脚本和 Kafka 功能验证，不用于生产或性能测试"
    else
        (( EXPECTED_DATA_DISK_GB >= 512 )) || die "生产规格 EXPECTED_DATA_DISK_GB 不能小于 512"
    fi
    validate_uint LOG_RETENTION_HOURS "$LOG_RETENTION_HOURS"
    validate_uint LOG_SEGMENT_BYTES "$LOG_SEGMENT_BYTES"
    validate_uint LOG_ROLL_HOURS "$LOG_ROLL_HOURS"
    validate_uint LOG_RETENTION_CHECK_INTERVAL_MS "$LOG_RETENTION_CHECK_INTERVAL_MS"
    validate_boolean DELETE_TOPIC_ENABLE "$DELETE_TOPIC_ENABLE"
    [[ "$LOG_MAX_SIZE" =~ ^[1-9][0-9]*[kKmMgG]$ ]] || die "LOG_MAX_SIZE 格式无效，例如 50m"
    validate_uint LOG_MAX_FILE "$LOG_MAX_FILE"
    validate_uint NOFILE_LIMIT "$NOFILE_LIMIT"

    local i j
    for ((i=0; i<3; i++)); do
        for ((j=i+1; j<3; j++)); do
            [[ "${controllers[$i]}" != "${controllers[$j]}" ]] || die "controller 地址不能重复: ${controllers[$i]}"
            [[ "${brokers[$i]}" != "${brokers[$j]}" ]] || die "broker 地址不能重复: ${brokers[$i]}"
        done
    done
    log_success "配置校验通过（规格: ${DEPLOY_PROFILE}）"
}
