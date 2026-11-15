#!/bin/bash
# Docker Environment Setup Script
# 独立的 Docker 安装和配置脚本，可在目标节点运行
# 作为 Kafka 等服务共用的基础环境工具
#
# Usage:
#   sudo ./setup-docker.sh                              # 交互模式（在线安装）
#   sudo ./setup-docker.sh --non-interactive            # 非交互模式（在线安装）
#   sudo ./setup-docker.sh --offline /path/to/pkgs      # 离线安装（从本地包目录）
#   sudo ./setup-docker.sh --offline /path/to/pkgs -y   # 离线 + 非交互
#   sudo ./setup-docker.sh --reconfigure                # 仅修改已安装 Docker 的配置
#   sudo ./setup-docker.sh --reconfigure --data-root /data2/docker -y  # 非交互修改配置
#
# Options:
#   --non-interactive, -y       非交互模式（使用默认值）
#   --offline <pkg-dir>         从本地包目录离线安装
#   --reconfigure               已安装时跳过安装，直接修改配置
#   --data-root <path>          指定 Docker data root（默认 /data1/docker）
#   --log-max-size <size>       指定日志最大大小（默认 100m）
#   --log-max-file <count>      指定日志最大文件数（默认 10）
#
# 离线包准备（在有网络的机器上执行）:
#   Ubuntu/Debian:
#     apt-get download docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
#   CentOS/RHEL:
#     yumdownloader --resolve docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

set -e

NON_INTERACTIVE=false
OFFLINE_PKG_DIR=""
RECONFIGURE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --non-interactive|-y)
            NON_INTERACTIVE=true
            shift
            ;;
        --offline)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "[ERROR] --offline requires a package directory path"
                exit 1
            fi
            OFFLINE_PKG_DIR="$2"
            shift 2
            ;;
        --reconfigure)
            RECONFIGURE=true
            shift
            ;;
        --data-root)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "[ERROR] --data-root requires a path"
                exit 1
            fi
            CUSTOM_DATA_ROOT="$2"
            shift 2
            ;;
        --log-max-size)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "[ERROR] --log-max-size requires a value"
                exit 1
            fi
            CUSTOM_LOG_MAX_SIZE="$2"
            shift 2
            ;;
        --log-max-file)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "[ERROR] --log-max-file requires a value"
                exit 1
            fi
            CUSTOM_LOG_MAX_FILE="$2"
            shift 2
            ;;
        *)
            echo "[ERROR] Unknown option: $1"
            echo "Usage: sudo ./setup-docker.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --non-interactive, -y       Non-interactive mode (use defaults)"
            echo "  --offline <pkg-dir>         Install from local packages"
            echo "  --reconfigure               Reconfigure Docker even if already installed"
            echo "  --data-root <path>          Set Docker data root (default: /data1/docker)"
            echo "  --log-max-size <size>       Set log max size (default: 100m)"
            echo "  --log-max-file <count>      Set log max files (default: 10)"
            exit 1
            ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info()     { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn()     { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error()    { echo -e "${RED}[ERROR]${NC} $1"; }
print_success()  { echo -e "${GREEN}[✓]${NC} $1"; }

get_input() {
    local prompt="$1" default="$2" var_name="$3"
    if [ "$NON_INTERACTIVE" = true ]; then
        printf -v "$var_name" '%s' "$default"
        return
    fi
    if [ -n "$default" ]; then
        read -p "$(echo -e "${BLUE}[?]${NC} $prompt [$default]: ")" value
        value="${value:-$default}"
    else
        read -p "$(echo -e "${BLUE}[?]${NC} $prompt: ")" value
    fi
    printf -v "$var_name" '%s' "$value"
}

validate_data_root() {
    local storage_path="$1"
    if [[ ! "$storage_path" =~ ^/[a-zA-Z0-9/._-]+$ || "$storage_path" == */ || "$storage_path" == *//* || "/${storage_path#/}/" == */./* || "/${storage_path#/}/" == */../* ]]; then
        print_error "Docker data-root 必须是规范的非根绝对路径，仅支持 ASCII 字母、数字、/._-，不含末尾斜杠或 .、.. 路径段"
        exit 1
    fi
}

check_root() {
    if [ "$EUID" -ne 0 ]; then
        print_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        OS_VERSION=$VERSION_ID
        print_info "Detected OS: $OS $OS_VERSION"
    else
        print_error "Cannot detect OS"
        exit 1
    fi
}

check_docker() {
    if command -v docker &> /dev/null; then
        DOCKER_VERSION=$(docker --version | awk '{print $3}' | sed 's/,//')
        print_info "Docker is already installed: $DOCKER_VERSION"
        return 0
    else
        print_warn "Docker is not installed"
        return 1
    fi
}

install_docker_offline() {
    print_info "Installing Docker from offline packages: $OFFLINE_PKG_DIR"

    if [ ! -d "$OFFLINE_PKG_DIR" ]; then
        print_error "Offline package directory not found: $OFFLINE_PKG_DIR"
        exit 1
    fi

    case $OS in
        ubuntu|debian)
            local deb_files=$(find "$OFFLINE_PKG_DIR" -name "*.deb" | sort)
            if [ -z "$deb_files" ]; then
                print_error "No .deb packages found in $OFFLINE_PKG_DIR"
                exit 1
            fi
            print_info "Found .deb packages:"
            echo "$deb_files" | while read f; do echo "  $(basename $f)"; done

            export DEBIAN_FRONTEND=noninteractive
            export NEEDRESTART_MODE=a
            export NEEDRESTART_SUSPEND=1
            dpkg -i "$OFFLINE_PKG_DIR"/*.deb 2>/dev/null || true
            # Fix any missing dependencies from local packages
            apt-get install -f -y \
                -o Dpkg::Options::="--force-confdef" \
                -o Dpkg::Options::="--force-confold" 2>/dev/null || true
            ;;
        centos|rhel|rocky|almalinux)
            local rpm_files=$(find "$OFFLINE_PKG_DIR" -name "*.rpm" | sort)
            if [ -z "$rpm_files" ]; then
                print_error "No .rpm packages found in $OFFLINE_PKG_DIR"
                exit 1
            fi
            print_info "Found .rpm packages:"
            echo "$rpm_files" | while read f; do echo "  $(basename $f)"; done

            rpm -ivh --replacepkgs --nodeps "$OFFLINE_PKG_DIR"/*.rpm 2>/dev/null || \
                yum localinstall -y "$OFFLINE_PKG_DIR"/*.rpm
            ;;
        *)
            print_error "Unsupported OS for offline install: $OS"
            exit 1
            ;;
    esac
    print_success "Docker installed from offline packages"
}

install_docker_online() {
    print_info "Installing Docker from online repositories..."
    case $OS in
        ubuntu|debian)
            export DEBIAN_FRONTEND=noninteractive
            export NEEDRESTART_MODE=a
            export NEEDRESTART_SUSPEND=1
            export APT_LISTCHANGES_FRONTEND=none
            echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

            apt-get update > /dev/null 2>&1
            apt-get install -y \
                -o Dpkg::Options::="--force-confdef" \
                -o Dpkg::Options::="--force-confold" \
                ca-certificates curl gnupg lsb-release > /dev/null 2>&1

            DOCKER_MIRROR=""
            if curl -s --connect-timeout 3 http://mirrors.tencentyun.com > /dev/null 2>&1; then
                DOCKER_MIRROR="https://mirrors.tencent.com/docker-ce"
            elif curl -s --connect-timeout 3 https://download.docker.com > /dev/null 2>&1; then
                DOCKER_MIRROR="https://download.docker.com"
            else
                DOCKER_MIRROR="https://mirrors.aliyun.com/docker-ce"
            fi

            mkdir -p /etc/apt/keyrings
            rm -f /etc/apt/keyrings/docker.gpg
            if ! curl -fsSL --connect-timeout 10 --max-time 30 "${DOCKER_MIRROR}/linux/$OS/gpg" | gpg --yes --dearmor -o /etc/apt/keyrings/docker.gpg 2>/dev/null; then
                if ! curl -fsSL --connect-timeout 10 --max-time 30 "https://mirrors.aliyun.com/docker-ce/linux/$OS/gpg" | gpg --yes --dearmor -o /etc/apt/keyrings/docker.gpg 2>/dev/null; then
                    print_error "Failed to download Docker GPG key"
                    exit 1
                fi
                DOCKER_MIRROR="https://mirrors.aliyun.com/docker-ce"
            fi

            echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] ${DOCKER_MIRROR}/linux/$OS $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
            apt-get update > /dev/null 2>&1
            apt-get install -y \
                -o Dpkg::Options::="--force-confdef" \
                -o Dpkg::Options::="--force-confold" \
                docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin > /dev/null 2>&1
            ;;
        centos|rhel|rocky|almalinux)
            yum install -y yum-utils
            yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        *)
            print_error "Unsupported OS: $OS"
            exit 1
            ;;
    esac
    print_success "Docker installed successfully"
}

install_docker() {
    if [ -n "$OFFLINE_PKG_DIR" ]; then
        install_docker_offline
    else
        install_docker_online
    fi
}

configure_docker() {
    print_info "Configuring Docker daemon..."

    # Use CLI args if provided, otherwise prompt/use defaults
    if [ -n "$CUSTOM_DATA_ROOT" ]; then
        DOCKER_DATA_ROOT="$CUSTOM_DATA_ROOT"
    else
        get_input "Docker data root directory" "/data1/docker" DOCKER_DATA_ROOT
    fi
    validate_data_root "$DOCKER_DATA_ROOT"

    if [ -n "$CUSTOM_LOG_MAX_SIZE" ]; then
        LOG_MAX_SIZE="$CUSTOM_LOG_MAX_SIZE"
    else
        get_input "Docker log max size (per container)" "100m" LOG_MAX_SIZE
    fi

    if [ -n "$CUSTOM_LOG_MAX_FILE" ]; then
        LOG_MAX_FILE="$CUSTOM_LOG_MAX_FILE"
    else
        get_input "Docker log max files (per container)" "10" LOG_MAX_FILE
    fi

    mkdir -p "$DOCKER_DATA_ROOT"

    if [ -f /etc/docker/daemon.json ]; then
        cp /etc/docker/daemon.json "/etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)"
    fi

    cat > /etc/docker/daemon.json <<EOF
{
  "data-root": "$DOCKER_DATA_ROOT",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "$LOG_MAX_SIZE",
    "max-file": "$LOG_MAX_FILE",
    "compress": "true"
  },
  "storage-driver": "overlay2",
  "live-restore": true,
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 65536, "Soft": 65536 }
  },
  "max-concurrent-downloads": 10,
  "max-concurrent-uploads": 10
}
EOF

    print_info "Docker daemon configured: data-root=$DOCKER_DATA_ROOT"
}

start_docker() {
    print_info "Starting Docker service..."
    systemctl daemon-reload
    systemctl enable docker
    systemctl restart docker
    sleep 3
    if systemctl is-active --quiet docker; then
        print_success "Docker service is running"
    else
        print_error "Failed to start Docker service"
        exit 1
    fi
}

verify_docker() {
    print_info "Verifying Docker installation..."
    docker --version
    if docker compose version &>/dev/null; then
        print_info "Docker Compose V2: $(docker compose version --short 2>/dev/null)"
    elif docker-compose --version &>/dev/null; then
        print_info "Docker Compose V1: $(docker-compose --version)"
    else
        print_warn "Docker Compose not found"
    fi
    docker info | grep -E "Server Version|Storage Driver|Docker Root Dir|Logging Driver"
}

main() {
    echo ""
    print_info "========================================="
    print_info "Docker Environment Setup"
    print_info "========================================="
    echo ""

    # Reject malformed CLI paths before installing packages or changing services.
    if [ -n "$CUSTOM_DATA_ROOT" ]; then
        validate_data_root "$CUSTOM_DATA_ROOT"
    fi
    check_root
    detect_os

    if check_docker; then
        if [ "$RECONFIGURE" = true ]; then
            print_info "Reconfiguring existing Docker installation..."
            configure_docker
            start_docker
            verify_docker
            echo ""
            print_success "Docker reconfiguration completed!"
            exit 0
        elif [ "$NON_INTERACTIVE" = false ]; then
            read -p "$(echo -e "${BLUE}[?]${NC} Docker is already installed. Reconfigure? [y/N]: ")" reconfigure
            if [[ ! "$reconfigure" =~ ^[Yy]$ ]]; then
                print_info "Skipping Docker installation"
                verify_docker
                exit 0
            fi
        else
            print_info "Docker already installed, skipping (use --reconfigure to update config)"
            exit 0
        fi
    else
        install_docker
    fi

    configure_docker
    start_docker
    verify_docker

    echo ""
    print_success "Docker setup completed!"
}

main
