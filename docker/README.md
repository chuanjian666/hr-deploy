# Docker 基础环境

`docker/` 与 `kafka/` 平级，用于在目标 Linux 节点安装和配置 Kafka 所需的 Docker 基础环境。Docker data-root 默认 `/data1/docker`，可通过交互或 `--data-root` 指定；Kafka 数据目录在 Kafka 初始化中独立指定，两者可以位于不同数据盘。Kafka 部署包和配置位于系统盘 `/root/deploy/kafka`。Kafka 节点脚本只检查 Docker，不修改其配置。

## 使用方式

以下以脚本位于 `/root/hr-deploy/docker`、Docker 数据目录为 `/data1/docker`、离线包目录为 `/root/docker-packages` 为例。路径不同时直接修改命令中的路径。

```bash
cd /root/hr-deploy/docker

# 在线交互安装
sudo /root/hr-deploy/docker/setup-docker.sh

# 在线非交互安装
sudo /root/hr-deploy/docker/setup-docker.sh --non-interactive

# 新装时指定 Docker 存储目录
sudo /root/hr-deploy/docker/setup-docker.sh --data-root /data1/docker -y

# 使用本地软件包离线安装
sudo /root/hr-deploy/docker/setup-docker.sh --offline /root/docker-packages -y

# 只重新配置已经安装的 Docker
sudo /root/hr-deploy/docker/setup-docker.sh --reconfigure --data-root /data1/docker -y
```

路径必须为规范的非根绝对路径，只支持 ASCII 字母、数字、`/._-`，不含末尾斜杠或 `.`、`..` 路径段。参数会在安装和修改配置前校验。`--data-root` 选择目录，`--reconfigure` 表示修改已有 Docker；已有 Docker 在非交互模式下未指定 `--reconfigure` 会跳过修改。脚本写入 `/etc/docker/daemon.json` 的 `data-root`，不会迁移旧目录的数据。

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
sudo /root/hr-deploy/docker/setup-docker.sh
```

Kafka 节点部署包随后执行：

```bash
/root/deploy/kafka/controller/deploy.sh check
/root/deploy/kafka/controller/deploy.sh prepare
/root/deploy/kafka/controller/deploy.sh start
```

`deploy.sh check` 的设计要求包括：

- `docker` 命令存在；
- Docker daemon 可访问且状态正常；
- `docker compose` V2 可用；
- 当前用户有权访问 Docker daemon；
- Docker 架构与 Kafka 镜像匹配；
- Docker data-root 由本脚本管理，Kafka 不配置或校验其路径；
- Kafka 根据 `KAFKA_DATA_BASE_DIR` 自动识别所在文件系统，生产环境要求独立挂载，使用率符合检查要求；
- 按实际占用为 Kafka 和 Docker 分别规划磁盘空间，不绑定固定容量；
- Kafka Compose 文件能够通过 `docker compose config --quiet` 校验。

Kafka 的 `manual` 规格允许 `KAFKA_DATA_BASE_DIR` 使用系统盘目录，默认 `/data1/kafka`，详细环境要求及验收步骤见 [本地最小功能验证](../kafka/docs/local-test.md)。

Kafka 的 `deploy.sh check` 需要在目标 Linux 节点的实际部署目录中执行，完整检查能力以脚本实现为准。静态校验只验证生成产物。

> `setup-docker.sh` 会修改 `/etc/docker/daemon.json` 并重启 Docker。已有 Docker 工作负载的节点应先审核配置和安排变更窗口。脚本会在覆盖配置前备份原文件。
