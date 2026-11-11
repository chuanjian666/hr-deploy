#!/usr/bin/env bash

[[ -n "${_KAFKA_CONFIG_SH_LOADED:-}" ]] && return 0
readonly _KAFKA_CONFIG_SH_LOADED=1

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

KAFKA_CONFIG_FILE="${KAFKA_CONFIG_FILE:-$(script_root)/config/kafka-admin.conf}"
KAFKA_CONFIG_EXAMPLE="$(script_root)/config/kafka-admin.conf.example"

ensure_config() {
    if [[ ! -f "$KAFKA_CONFIG_FILE" ]]; then
        cp "$KAFKA_CONFIG_EXAMPLE" "$KAFKA_CONFIG_FILE"
        chmod 600 "$KAFKA_CONFIG_FILE"
    fi
}

load_config() {
    ensure_config
    # 兼容尚未包含认证选项的旧配置文件。
    ENABLE_AUTH=false
    AUTH_USERNAME=admin
    AUTH_PASSWORD=""
    # 配置文件是本工具管理的受信任 Bash 文件。
    # shellcheck disable=SC1090
    source "$KAFKA_CONFIG_FILE"
}

get_config() { printf '%s\n' "${!1:-${2:-}}"; }

validate_config_key() {
    [[ "$1" =~ ^[A-Z][A-Z0-9_]*$ ]] || die "非法配置键: $1"
    grep -qE "^${1}=" "$KAFKA_CONFIG_EXAMPLE" || die "不支持的配置键: $1"
}

save_config() {
    local key="$1" value="$2" encoded tmp
    validate_config_key "$key"
    require_single_line "$key" "$value"
    printf -v encoded '%q' "$value"
    tmp="$(mktemp "${KAFKA_CONFIG_FILE}.tmp.XXXXXX")"
    KAFKA_ENCODED="$encoded" awk -v key="$key" '
        BEGIN { value=ENVIRON["KAFKA_ENCODED"]; replaced=0 }
        $0 ~ ("^" key "=") { print key "=" value; replaced=1; next }
        { print }
        END { if (!replaced) print key "=" value }
    ' "$KAFKA_CONFIG_FILE" > "$tmp"
    chmod 600 "$tmp"
    mv "$tmp" "$KAFKA_CONFIG_FILE"
    export "$key=$value"
}

show_config() {
    ensure_config
    awk '/^AUTH_PASSWORD=/ {print "AUTH_PASSWORD=<redacted>"; next} !/^[[:space:]]*$/ {print}' "$KAFKA_CONFIG_FILE"
}

reset_config() {
    cp "$KAFKA_CONFIG_EXAMPLE" "$KAFKA_CONFIG_FILE"
    chmod 600 "$KAFKA_CONFIG_FILE"
    load_config
    log_success "配置已重置: $KAFKA_CONFIG_FILE"
}
