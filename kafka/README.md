# Kafka 部署工具

用于生成 3 个 KRaft controller 和 3 个 broker 的独立 Docker Compose 节点包。支持可选客户端 SCRAM 账号认证，内部通信始终使用 PLAINTEXT，默认不启用 TLS 和 ACL，并关闭 Topic 自动创建。

## 部署指南

- [本地手动部署](docs/deploy-local.md)：在测试宿主机准备三个模拟节点，逐步生成、分发、初始化、启动和验收。
- [生产部署](docs/deploy-production.md)：在三台 Linux 服务器上准备环境、部署六个角色，执行状态检查和专用 Topic 读写验收。
- [部署后消息收发验证](docs/verify-messages.md)：独立手动验证文档，本地和生产使用相同的 Topic、生产、消费及内容比对命令。
- [部署后启用 ACL 和账号授权](docs/enable-acl-after-deploy.md)：已认证集群的权限切换、账号授权、正反向验证及回退。
- [功能验证要求](docs/local-test.md)：测试环境约束、自动化入口和验收记录要求。
- [本地执行目录](docs/variables.md)：本机执行位置及固定节点地址。

## 基础环境

目标节点先准备 Docker。以下以仓库位于 `/root/hr-deploy` 为例，路径不同时直接修改命令。

```bash
sudo /root/hr-deploy/docker/setup-docker.sh --data-root /data1/docker
```

## 集群初始化

在管理机执行，仓库路径以 `/root/hr-deploy` 为例：

```bash
cd /root/hr-deploy/kafka
/root/hr-deploy/kafka/kafka-admin.sh cluster init
/root/hr-deploy/kafka/kafka-admin.sh cluster validate
```

按提示输入集群名称、镜像、部署规格、三个 controller 和三个 broker 的地址、Kafka 数据基础目录及认证设置。地址顺序决定节点编号。首次运行会自动创建 `config/kafka-admin.conf`，再次运行时可沿用括号内的值，manual 资源项需要显式输入。三种规格的节点包都生成到 `cluster/nodes/`。

初始化时可选择 `ENABLE_AUTH=true`，确认初始账号和客户端端口（默认 19092）。首次密码留空会自动生成 6 位随机密码并保存到管理机配置及 `cluster/clients/admin.properties`，不在终端显示；手动指定密码不限制长度。重复初始化沿用配置中的密码，不自动轮换。开启后所有业务客户端使用 `SASL_PLAINTEXT + SCRAM-SHA-512` 连接该端口；9092/9093 仅允许集群节点互访。管理机需要 Python 3，无需第三方 Python 包；目标节点不需要 Python。

初始凭据仅在 controller 首次格式化时写入。独立生产者/消费者账号可在集群启动后动态创建。配置、账号管理及已有集群切换步骤见 [账号认证说明](docs/authentication.md)。

自动化可预先填写 `/root/hr-deploy/kafka/config/kafka-admin.conf`，通过非终端输入执行 `cluster init` 时会读取保存的配置。

同一集群重复初始化会复用 `cluster/manifest.json` 中的 cluster ID 和 controller directory ID。沿用旧部署时，若节点包原在 `generated/local-test/cluster/` 或 `generated/production/cluster/`，请在首次使用统一目录前将该集群完整的旧 `cluster/` 移到空缺的 `kafka/cluster/`，并将配套管理配置放在 `kafka/config/kafka-admin.conf`。若目标已有另一套配置或清单，先核对集群身份，避免覆盖。

## 节点操作

将每个节点包复制到对应目标节点。以下以第一个节点 `10.0.0.11` 为例，将示例地址直接替换为实际地址：

```bash
ssh root@10.0.0.11 'mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker'
scp -r /root/hr-deploy/kafka/cluster/nodes/controller-1-10.0.0.11/. root@10.0.0.11:/root/deploy/kafka/controller/
scp -r /root/hr-deploy/kafka/cluster/nodes/broker-1-10.0.0.11/. root@10.0.0.11:/root/deploy/kafka/broker/
```

在目标节点执行：

```bash
/root/deploy/kafka/controller/deploy.sh check
/root/deploy/kafka/controller/deploy.sh prepare
/root/deploy/kafka/controller/deploy.sh start
/root/deploy/kafka/controller/deploy.sh status
```

三个 controller 均发起启动后，在任一 controller 包中执行：

```bash
/root/deploy/kafka/controller/deploy.sh health       # 单次检查 controller 集群，成功返回 0
/root/deploy/kafka/controller/deploy.sh health 120   # 轮询等待健康，失败返回非零
```

检查三个 controller 入口、cluster ID、voter 清单、leader 一致性及全部 voter 的复制 lag=0。检查不依赖 broker，使用临时 CLI 容器，不重启服务或修改数据；`status` 仍只显示容器状态。初次部署应在此检查通过后启动 broker；`health` 只适用于 controller 包。

`prepare` 会格式化 KRaft 数据目录，是首次部署操作。脚本发现已有且身份一致的 `meta.properties` 时跳过；身份冲突或非空但未格式化的目录会拒绝处理。

## 部署规格

| 规格 | 每台 VM | controller | broker | OS/Docker 预留 | 用途 |
| --- | --- | --- | --- | --- | --- |
| standard | 8C32G | 1C2G，heap 1G | 6C24G，heap 8G | 1C6G | 标准生产 |
| minimal | 4C16G | 1C2G，heap 1G | 2C10G，heap 4G | 1C4G | 最小生产，需压测 |
| manual | 按实际主机 | 输入 CPU 和内存，heap 为内存的 1/2 | 输入 CPU 和内存，heap 为内存的 1/3 | 按实际主机预留 | 自定义角色资源 |

选择 `manual` 后，每次交互初始化都要分别输入每个 controller 和每个 broker 的 CPU 核数与内存上限，共四项。CPU 支持小数，内存接受整数加 `m`（MiB）或 `g`（GiB），heap 向下取整到 MiB。非交互初始化须在管理机配置中填写 `MANUAL_CONTROLLER_CPU`、`MANUAL_CONTROLLER_MEMORY`、`MANUAL_BROKER_CPU` 和 `MANUAL_BROKER_MEMORY`。

三个规格只限定 CPU、内存和 heap，不限定磁盘容量或介质；磁盘按实际流量、保留期、副本数和余量规划。生产两档要求数据所在文件系统独立挂载，脚本根据数据路径自动识别；`manual` 可使用系统盘目录。本地模拟环境使用的 20 GiB 测试盘是测试环境设置，不是规格下限。默认目录布局如下，Kafka 和 Docker 的存储目录可以独立设置：

```text
/data1/docker                         Docker data-root
/data1/kafka/controller/metadata   controller 数据
/data1/kafka/broker/data           broker 数据
/root/deploy/kafka/<role>          部署包和配置
```

每台目标节点的目录不带节点编号；管理机上的包名、Kafka node ID、容器名和 Compose 项目名仍带编号，以区分节点。三个同角色实例必须分发到各自的主机。

Kafka 默认保留 7 天数据。完整方案见 [docs/plan.md](docs/plan.md)。

## 自定义宿主机存储目录

Docker 在每台目标机通过自己的脚本设置；新装时将实际路径通过 `--data-root /data1/docker` 传入。已安装 Docker 时增加 `--reconfigure`，该操作会重启 Docker，不会迁移旧数据。

在管理机执行 `/root/hr-deploy/kafka/kafka-admin.sh cluster init`，在 Kafka 数据基础目录提示中输入实际的 `KAFKA_DATA_BASE_DIR` 路径。

只需配置 `KAFKA_DATA_BASE_DIR`，无需另填挂载点；各角色自动生成子目录，如 `/data1/kafka/broker/data`。路径使用规范的非根绝对路径，支持 ASCII 字母、数字和 `/._-`，不含空格、末尾斜杠或 `.`、`..` 路径段。Docker 存储目录完全由 Docker 部署脚本管理；Kafka 只检查 Docker daemon 和 Compose 可用，不采集或限制 Docker data-root。三个节点采用同一组目录配置，磁盘可以不同；部署包根目录仍固定为 `/root/deploy/kafka`。

配置统一使用 `KAFKA_DATA_BASE_DIR`。节点检查通过 `findmnt -T` 识别实际文件系统；数据目录未创建时检查最近的已有父目录，生产环境拒绝落入系统根文件系统。

测试环境约束、操作步骤和验收要求见 [docs/local-test.md](docs/local-test.md)。

## 当前边界

- Kafka 管理、格式化、健康检查和消息验证工具统一通过独立的 `docker run --rm` 执行，默认容器内存上限 512 MiB、JVM 堆 128–256 MiB，并限制额外 swap；不在 broker/controller 服务容器内用 `docker exec` 启动 CLI JVM。配置及操作示例见 [账号认证](docs/authentication.md#kafka-cli-执行方式和内存限制)。长期运行的 Kafka 服务仍由 Compose 按角色规格管理。
- `cluster validate` 只做静态校验，不拉取镜像、不启动容器。
- `deploy.sh check` 需要在目标 Linux 节点以 root 执行。
- Compose 和 storage format 使用 `apache/kafka:4.3.2` 镜像内的 `/opt/kafka/bin` 路径；正式部署前仍应在目标环境确认镜像架构及路径。
- 默认未启用 ACL，此时即使开启认证也不能限制只有管理员才能显式创建 Topic，各账号权限相同。部署后可按 [ACL 操作文档](docs/enable-acl-after-deploy.md) 手动启用权限控制。
