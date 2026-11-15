#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export _KAFKA_SCRIPT_ROOT="$SCRIPT_DIR"

source "$SCRIPT_DIR/scripts/lib/common.sh"
source "$SCRIPT_DIR/scripts/lib/config.sh"
source "$SCRIPT_DIR/scripts/lib/validation.sh"
source "$SCRIPT_DIR/scripts/core/cluster-engine.sh"

VERSION="1.0.0"

show_help() {
    cat <<EOF
Kafka Admin CLI v$VERSION

用法:
  $0 cluster init              交互确认配置并生成 6 个角色节点包
  $0 cluster info              显示已生成的集群清单
  $0 cluster validate          静态校验生成产物，不连接或部署 Kafka

  $0 config show               显示管理机配置
  $0 config validate           校验管理机配置
  $0 config set KEY VALUE      修改一个支持的配置项
  $0 config reset              重置为示例配置

  $0 help
  $0 version

目标节点收到角色包后使用 ./deploy.sh check|prepare|start|stop|restart|status|logs。
controller 包另支持 ./deploy.sh health [等待秒数] 检查 controller 集群健康。
EOF
}

main() {
    local command="${1:-help}" subcommand="${2:-}"
    case "$command" in
        cluster)
            case "$subcommand" in
                init) cluster_init ;;
                info) cluster_info ;;
                validate) cluster_verify_static ;;
                *) die "未知 cluster 命令: ${subcommand:-<empty>}" ;;
            esac
            ;;
        config)
            case "$subcommand" in
                show) show_config ;;
                validate) load_config; validate_cluster_config ;;
                set)
                    [[ -n "${3:-}" && -n "${4:-}" ]] || die "用法: $0 config set KEY VALUE"
                    ensure_config; save_config "$3" "$4"; log_success "配置已更新: $3"
                    ;;
                reset) reset_config ;;
                *) die "未知 config 命令: ${subcommand:-<empty>}" ;;
            esac
            ;;
        help|--help|-h) show_help ;;
        version|--version|-v) echo "Kafka Admin CLI v$VERSION" ;;
        *) show_help; die "未知命令: $command" ;;
    esac
}

main "$@"
