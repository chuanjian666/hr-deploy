#!/usr/bin/env bash

[[ -n "${_KAFKA_COMMON_SH_LOADED:-}" ]] && return 0
readonly _KAFKA_COMMON_SH_LOADED=1

COLOR_RED='\033[0;31m'
COLOR_GREEN='\033[0;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

log_info() { printf "${COLOR_BLUE}[INFO]${COLOR_RESET} %s\n" "$*" >&2; }
log_warn() { printf "${COLOR_YELLOW}[WARN]${COLOR_RESET} %s\n" "$*" >&2; }
log_error() { printf "${COLOR_RED}[ERROR]${COLOR_RESET} %s\n" "$*" >&2; }
log_success() { printf "${COLOR_GREEN}[SUCCESS]${COLOR_RESET} %s\n" "$*" >&2; }
die() { log_error "$@"; exit 1; }

script_root() { printf '%s\n' "${_KAFKA_SCRIPT_ROOT:?未设置 _KAFKA_SCRIPT_ROOT}"; }
ensure_dir() { mkdir -p "$1"; }
require_command() { command -v "$1" >/dev/null 2>&1 || die "缺少命令: $1"; }
require_single_line() {
    [[ "$2" != *$'\n'* && "$2" != *$'\r'* ]] || die "$1 不能包含换行或回车"
}

shell_quote() { printf '%q' "$1"; }
sha256_file() { sha256sum "$1" | awk '{print $1}'; }

json_escape() {
    local value="$1"
    value=${value//\\/\\\\}
    value=${value//\"/\\\"}
    value=${value//$'\n'/\\n}
    value=${value//$'\r'/\\r}
    value=${value//$'\t'/\\t}
    printf '%s' "$value"
}

atomic_replace_dir() {
    local source_dir="$1" target_dir="$2" backup_dir=""
    if [[ -e "$target_dir" ]]; then
        backup_dir="${target_dir}.previous.$$"
        mv "$target_dir" "$backup_dir"
    fi
    if mv "$source_dir" "$target_dir"; then
        [[ -z "$backup_dir" ]] || rm -rf "$backup_dir"
    else
        [[ -z "$backup_dir" || ! -e "$backup_dir" ]] || mv "$backup_dir" "$target_dir"
        return 1
    fi
}
