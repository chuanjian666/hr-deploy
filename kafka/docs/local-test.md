# 本地最小功能验证

## 资源与范围

`test` 用于验证部署脚本和 Kafka 基础功能，不用于生产或性能测试。准备 3 台互通的 Linux VM，每台 2 vCPU / 4 GiB 内存、至少 20 GB 磁盘（含系统、镜像和少量测试消息）；合计 6 vCPU / 12 GiB / 60 GB，宿主机另需保留自身资源。每台 VM 共置一个 controller 和一个 broker，仍生成 6 个独立角色包。已在本地 3 个隔离 Linux 测试节点中通过功能验证，独立 VM 验收仍需单独执行，详见 [验证记录](#验证记录)。

| 每台节点用途 | CPU limit | memory limit | JVM heap |
| --- | --- | --- | --- |
| controller | 0.5 vCPU | 512 MiB | 256 MiB |
| broker | 1 vCPU | 1536 MiB | 512 MiB |
| OS、Docker 与临时 CLI 预留 | 0.5 vCPU | 2 GiB | — |

## 三台 Linux VM 手动验证

每台 VM 创建 `/data1` 目录并使用 [Docker 安装脚本](../../docker/README.md)准备 Docker Engine 和 Compose v2，Docker data-root 仍为 `/data1/docker`。`test` 允许 `/data1` 是系统盘目录，无需独立挂载；部署检查时至少需要 5 GB 可用空间。本方案的 host 网络要求 3 台 Linux VM，不能将 3 个地址全部填写为同一个本地地址来代替三节点。

### 生成部署包

在仓库 `kafka/` 目录执行管理机配置（地址替换为实际 VM IP）：

```bash
./kafka-admin.sh config set CLUSTER_NAME hr-kafka-test
./kafka-admin.sh config set DEPLOY_PROFILE test
./kafka-admin.sh config set EXPECTED_DATA_DISK_GB 20
./kafka-admin.sh config set CONTROLLER_HOSTS '10.20.0.11,10.20.0.12,10.20.0.13'
./kafka-admin.sh config set BROKER_HOSTS '10.20.0.11,10.20.0.12,10.20.0.13'
KAFKA_INTERACTIVE=false ./kafka-admin.sh cluster init
./kafka-admin.sh cluster validate
```

在独立测试目录初始化，避免复用其他集群的 `cluster/manifest.json`；也可通过 `KAFKA_CONFIG_FILE` 和 `KAFKA_CLUSTER_DIR` 指定独立配置和产物目录。`test` 只降低容器资源和磁盘门槛，仍保留 3 controller、3 broker、3 副本、min ISR 2、关闭 Topic 自动创建及身份与文件摘要校验。配置默认值仍为 `standard` 和 512 GB，切换 `test` 时须同时设置磁盘容量。

### 部署与基础验收

按 [节点操作](../README.md#节点操作) 分发部署包，在 6 个角色包中执行 `check`、`prepare`，先启动 controller 再启动 broker。部署完成后，在任一 broker 包目录中执行以下基础验收，不运行性能测试工具：

```bash
# 将地址替换为实际 VM IP；CLI 使用较小的临时 heap。
BOOTSTRAP='10.20.0.11:9092,10.20.0.12:9092,10.20.0.13:9092'
kcli() {
    local tool="$1"; shift
    docker compose exec -T -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
        -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
        kafka "/opt/kafka/bin/$tool" "$@"
}

# 确认 quorum 有 leader、3 个 voter，3 个 controller 复制正常。
kcli kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status
kcli kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --replication
# 确认输出包含全部 3 个 broker。
kcli kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP"

TOPIC="deploy-smoke-$(date +%s)"
kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create \
    --topic "$TOPIC" --partitions 3 --replication-factor 3 --config min.insync.replicas=2
# 等待每个分区都有 leader，且 Replicas/Isr 均包含 3 个 broker 后再继续。
kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
printf 'deploy-smoke-ok\n' | kcli kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --producer-property acks=all
kcli kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" --from-beginning --max-messages 1 --timeout-ms 30000
```

消费结果应为 `deploy-smoke-ok`。再对当前 broker 执行 `./deploy.sh restart`，待分区 ISR 恢复为 3 后重新执行消费命令，确认消息仍在；重复 `./deploy.sh prepare` 应提示身份一致并跳过格式化。验证访问不存在的 Topic 不会自动创建时，向一个新的 Topic 使用带短超时的 producer 发送消息，再通过 `kafka-topics.sh --list` 确认该 Topic 不存在。验收以 quorum、broker、ISR 和读写结果为准，`deploy.sh status` 的容器状态不能替代这些检查。只写入少量测试消息，完成后通过各角色的 `./deploy.sh stop` 停止服务，保留数据以便复验。

## 本机自动验证

只有 Docker、没有准备好 3 台 Linux VM 时，可使用独立 Docker 守护进程模拟三个 Linux 节点。每个节点限制为 2 核 / 4 GiB，并挂载独立的 20 GiB ext4 测试盘；节点内部直接执行生成的 `deploy.sh`，不挂载宿主 Docker socket，不修改宿主 Docker data-root。测试节点使用 privileged 容器以运行嵌套 Docker 和挂载测试盘，共享宿主内核，不能替代三台独立 VM 的故障隔离验收。

在仓库根目录执行，需要 Docker daemon 权限和 Python 3：

```bash
bash kafka/tests/test-local.sh up
bash kafka/tests/test-local.sh prepare
bash kafka/tests/test-local.sh start
bash kafka/tests/test-local.sh verify
bash kafka/tests/test-local.sh status
# 验证后停止，保留三个测试盘及日志。
bash kafka/tests/test-local.sh stop
```

`up` 会拉取公开测试节点镜像、构建依赖环境，并向各节点导入同一 Kafka 镜像。默认网络为 `172.30.93.0/24`；如与本地网络冲突，需要同步调整 `tests/local/docker-compose.yml` 和 `tests/test-local.sh` 的地址。配置、生成包、日志及 `result.json` 位于 Git 忽略的 `generated/local-test/`。只有 `verify` 返回成功，且结果文件显示 `status=passed`，才能判定该次功能验收通过。

## 验证记录

2026-11-10 23:23（Asia/Shanghai）完成，结果通过。未执行性能测试。

### 环境与范围

宿主为 macOS arm64，使用现有 Docker Desktop。创建专用 `hr-kafka-local-test` 网络和 3 个 Docker-in-Docker Linux 节点，每个节点有独立 Docker 28.5.2 守护进程、2 vCPU / 4 GiB 容器限制和独立 20 GiB ext4 测试盘。每节点共置一个独立 controller 和一个独立 broker，直接运行工具生成的部署包。

| 项目 | 实际值 |
| --- | --- |
| Kafka | `apache/kafka:4.3.2`，arm64，CLI 实测版本 4.3.2 |
| 镜像 digest | `sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7` |
| 测试地址 | `172.30.93.11`、`172.30.93.12`、`172.30.93.13` |
| controller | 0.5 vCPU / 512 MiB，heap 256 MiB |
| broker | 1 vCPU / 1536 MiB，heap 512 MiB |
| cluster ID | `G4T4aVB6u4qU9y_bOur5ew` |
| controller leader | 101，voter 为 101、102、103，检查时 lag 均为 0 |
| 验收 Topic | `deploy-smoke-20261110t152210z` |

三个节点共享 Docker Desktop 的 Linux 内核，测试盘不包含外层节点镜像，不能等同于三台独立 VM、每台系统盘总预算 20 GB 的完整验证。未修改宿主 Docker data-root 或已有服务，没有对宿主暴露 Kafka 端口。

### 已通过的验证

- 6 个角色的 `check`、首次 `prepare`、`start` 和 `status`。
- 实际容器 CPU/内存限制符合 `test` 档，最终 6 个角色均运行且未被 OOM kill。
- quorum 有 leader，3 个 controller voter 均同步，3 个 broker 均注册。
- 创建 3 分区、3 副本、min ISR 2 的 Topic，各分区 ISR 均为 3。
- producer 使用 `acks=all` 发送 3 条消息，consumer 逐条内容比对成功。
- 对不存在的 Topic 发送消息按短超时失败，随后确认该 Topic 未被自动创建。
- 6 个角色重复 `prepare` 均跳过格式化，`meta.properties` 的 SHA-256 均不变。
- broker-1 执行 `restart` 后 ISR 恢复为 3，原有 3 条消息可再次完整消费。
- broker-1 执行 `stop` / `start` 重建容器后 ISR 恢复为 3，原有消息仍可完整消费。

### 发现和修复

首次真实 `prepare` 报 `AccessDeniedException: /etc/kafka/custom/server.properties`。原因是配置及数据目录由 root 创建，而 Kafka 镜像以 `appuser`（1000:1000）运行。`prepare` 现从实际镜像解析 UID/GID，修复配置及数据目录所有者；保持配置只读挂载和受限权限，不以 root 运行 Kafka。首次格式化和重复 prepare 均已实测通过。

Kafka CLI 原先继承服务的 stdout 日志配置，导致消费输出混入日志。验收命令现显式使用镜像内 `tools-log4j2.yaml`，使消息输出可精确比对。storage format 使用小型临时 heap，并在返回后确认 `meta.properties` 确实存在。

### 复验与产物

复验命令见 [本机自动验证](#本机自动验证)。

本次通过后保留集群运行。生成包位于 `kafka/generated/local-test/cluster/`，机器可读结果为 `kafka/generated/local-test/result.json`，各阶段及 Kafka CLI 原始输出位于 `kafka/generated/local-test/logs/`，均被 Git 忽略。

当前会话因 Docker Desktop 凭据助手停滞，使用 `generated/local-test/docker-client/` 下无凭据的独立配置执行公开镜像操作，未修改用户 Docker 配置。若同样遇到该问题，可先记录当前 context 的 Docker endpoint，再通过 `DOCKER_HOST` 和 `DOCKER_CONFIG` 环境变量指定该 endpoint 与独立配置。

未验证三台独立 VM 的重启和故障隔离、生产规格运行、amd64、完整事务流程及任何容量或性能指标。所有结论仅对应本次功能测试。
