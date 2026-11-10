# Docker 基础环境

`docker/` 与 `kafka/` 平级，用于在目标 Linux 节点安装和配置 Kafka 所需的 Docker 基础环境。默认将 Docker data-root 配置为 `/data1/docker`，与 Kafka 持久化目录 `/data1/kafka` 共享同一块数据盘但使用独立子目录。Kafka 部署包和配置位于系统盘 `/root/deploy/kafka`。Docker 安装与 Kafka 部署相互独立，Kafka 节点脚本只检查依赖，不自动修改宿主机 Docker 配置。

## 使用方式

```bash
# 在线交互安装
sudo ./setup-docker.sh

# 在线非交互安装
sudo ./setup-docker.sh --non-interactive

# 使用本地软件包离线安装
sudo ./setup-docker.sh --offline /path/to/packages -y

# 只重新配置已经安装的 Docker
sudo ./setup-docker.sh --reconfigure --data-root /data1/docker -y
```

## 脚本能力

- 检测 Ubuntu、Debian、CentOS、RHEL、Rocky Linux 和 AlmaLinux。
- 安装 Docker Engine、containerd、Buildx 和 Docker Compose 插件。
- 支持 `.deb` 或 `.rpm` 软件包离线安装。
- 配置 Docker data-root、`json-file` 日志轮转、`overlay2`、live restore 和默认 `nofile`。
- 启用并重启 Docker daemon。
- 输出 Docker、Compose、存储驱动、日志驱动和 data-root 信息。

## 与 Kafka 部署的边界

Docker 环境准备：

```bash
cd docker
sudo ./setup-docker.sh
```

Kafka 节点部署包随后执行：

```bash
./deploy.sh check
./deploy.sh prepare
./deploy.sh start
```

`deploy.sh check` 的设计要求包括：

- `docker` 命令存在；
- Docker daemon 可访问且状态正常；
- `docker compose` V2 可用；
- 当前用户有权访问 Docker daemon；
- Docker 架构与 Kafka 镜像匹配；
- Docker data-root 为 `/data1/docker`，且 `/data1` 已正确挂载；
- 共享数据盘的总容量和剩余空间满足 Docker 与 Kafka 的合计要求；
- Kafka Compose 文件能够通过 `docker compose config --quiet` 校验。

Kafka 的 `test` 规格允许 `/data1` 使用系统盘目录，详细环境要求及验收步骤见 [本地最小功能验证](../kafka/docs/local-test.md)。

Kafka 的 `deploy.sh check` 需要在目标 Linux 节点的实际部署目录中执行，完整检查能力以脚本实现为准。静态校验只验证生成产物。

> `setup-docker.sh` 会修改 `/etc/docker/daemon.json` 并重启 Docker。已有 Docker 工作负载的节点应先审核配置和安排变更窗口。脚本会在覆盖配置前备份原文件。
