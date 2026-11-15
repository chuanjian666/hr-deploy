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
    [[ "$2" =~ ^/[a-zA-Z0-9/._-]+$ && "$2" != */ && "$2" != *//* && "/${2#/}/" != */./* && "/${2#/}/" != */../* ]] || \
        die "$1 必须是规范的非根绝对路径，仅支持 ASCII 字母、数字、/._-，不含末尾斜杠或 .、.. 路径段: $2"
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

memory_mib() {
    local size="${1%?}"
    case "$1" in
        *[gG]) echo "$((size * 1024))" ;;
        *) echo "$size" ;;
    esac
}

valid_manual_resource() {
    local key="$1" value="$2"
    case "$key" in
        *_CPU)
            [[ "$value" =~ ^(0|[1-9][0-9]{0,8})([.][0-9]{1,9})?$ ]] &&
                LC_ALL=C awk -v cpu="$value" 'BEGIN { exit !(cpu >= 0.01) }'
            ;;
        *_MEMORY)
            [[ "$value" =~ ^[1-9][0-9]{0,8}[mMgG]$ ]] && (( $(memory_mib "$value") >= 6 ))
            ;;
    esac
}

validate_cluster_config() {
    validate_name CLUSTER_NAME "$CLUSTER_NAME"
    validate_image KAFKA_IMAGE "$KAFKA_IMAGE"
    case "$DEPLOY_PROFILE" in
        standard|minimal) ;;
        manual)
            local key
            for key in MANUAL_CONTROLLER_CPU MANUAL_CONTROLLER_MEMORY MANUAL_BROKER_CPU MANUAL_BROKER_MEMORY; do
                valid_manual_resource "$key" "${!key:-}" || die "请填写有效的 $key（CPU 至少 0.01 核，内存如 512m 或 2g，至少 6 MiB）"
            done
            ;;
        test) die "test 已改为 manual，请运行交互式 cluster init 输入两个角色的 CPU 和内存" ;;
        *) die "DEPLOY_PROFILE 必须是 standard、minimal 或 manual" ;;
    esac
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
        [[ "$AUTH_PASSWORD" =~ ^[a-zA-Z0-9_@%+=:./-]+$ ]] || die "AUTH_PASSWORD 不能为空，仅支持 ASCII 字母、数字或 _@%+=:./-"
    fi
    if [[ "$ENABLE_EXTERNAL_LISTENER" == true || "$ENABLE_AUTH" == true ]]; then
        [[ "$EXTERNAL_PORT" != "$CONTROLLER_PORT" && "$EXTERNAL_PORT" != "$BROKER_PORT" ]] || die "EXTERNAL_PORT 不能与其他端口相同"
    fi
    validate_absolute_path TARGET_DEPLOY_ROOT "$TARGET_DEPLOY_ROOT"
    validate_absolute_path KAFKA_DATA_BASE_DIR "$KAFKA_DATA_BASE_DIR"
    [[ "$TARGET_DEPLOY_ROOT" == /root/deploy/kafka ]] || die "TARGET_DEPLOY_ROOT 当前固定为 /root/deploy/kafka"
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
