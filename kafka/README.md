# Kafka 部署工具

用于生成 3 个 KRaft controller 和 3 个 broker 的独立 Docker Compose 节点包。当前配置使用 PLAINTEXT，不启用认证和 ACL，并关闭 Topic 自动创建。

## 基础环境

目标节点先准备 Docker：

```bash
cd ../docker
sudo ./setup-docker.sh --data-root /data1/docker
```

## 集群初始化

复制并编辑配置：

```bash
cd kafka
cp config/kafka-admin.conf.example config/kafka-admin.conf
chmod 600 config/kafka-admin.conf
# 必填 CONTROLLER_HOSTS 和 BROKER_HOSTS，各填写 3 个逗号分隔地址
```

也可以交互生成：

```bash
./kafka-admin.sh cluster init
```

非交互生成适合自动化和基本测试：

```bash
KAFKA_INTERACTIVE=false ./kafka-admin.sh cluster init
./kafka-admin.sh cluster validate
```

生成产物位于 `cluster/`。重复执行会复用 `cluster/manifest.json` 中的 cluster ID 和 controller directory ID。

## 节点操作

将每个节点包复制到对应目标节点，例如：

```bash
scp -r cluster/nodes/controller-1-10.0.0.11 root@10.0.0.11:/root/deploy/kafka/controller-1
scp -r cluster/nodes/broker-1-10.0.0.11 root@10.0.0.11:/root/deploy/kafka/broker-1
```

在目标节点执行：

```bash
./deploy.sh check
./deploy.sh prepare
./deploy.sh start
./deploy.sh status
```

`prepare` 会格式化 KRaft 数据目录，是首次部署操作。脚本发现已有且身份一致的 `meta.properties` 时跳过；身份冲突或非空但未格式化的目录会拒绝处理。

## 固定规格

| 规格 | VM | controller | broker | OS/Docker 预留 |
| --- | --- | --- | --- | --- |
| standard | 8C32G | 1C2G，heap 1G | 6C24G，heap 8G | 1C6G |
| minimal | 4C16G | 1C2G，heap 1G | 2C10G，heap 4G | 1C4G |

每台使用一块挂载到 `/data1` 的 512 GB HDD：

```text
/data1/docker                         Docker data-root
/data1/kafka/controller-N/metadata   controller 数据
/data1/kafka/broker-N/data           broker 数据
/root/deploy/kafka/<role>-N          部署包和配置
```

Kafka 默认保留 7 天数据。完整方案见 [docs/plan.md](docs/plan.md)。

## 当前边界

- `cluster validate` 只做静态校验，不拉取镜像、不启动容器。
- `deploy.sh check` 需要在目标 Linux 节点以 root 执行。
- Compose 和 storage format 使用 `apache/kafka:4.3.2` 镜像内的 `/opt/kafka/bin` 路径；正式部署前仍应在目标环境确认镜像架构及路径。
- 没有认证和 ACL 时，Kafka 无法限制只有管理员才能显式创建 Topic。
