# Kafka 本地手动部署指南

在本机通过三个 Docker-in-Docker 容器模拟 Linux 节点，每个节点同时运行一个 controller 和一个 broker。节点共享 Docker 运行环境的 Linux 内核，每个节点上限为 2 核 CPU / 4 GiB 内存，配有独立的 20 GiB 测试盘。本流程用于部署功能验证，验收要求见 [local-test.md](local-test.md)。

## 1. 进入本地仓库

先启动 Docker Desktop，在本机终端进入仓库根目录。将下面的示例替换为实际路径。后续本机命令使用当前目录 `$PWD`，执行时保持位于仓库根目录。

```bash
cd /path/to/hr-deploy
```

新终端同样先进入仓库根目录。本文采用以下固定地址和目录。

| 模拟节点 | 固定 IP | 角色 |
| --- | --- | --- |
| node-1 | 172.30.93.11 | controller-1、broker-1 |
| node-2 | 172.30.93.12 | controller-2、broker-2 |
| node-3 | 172.30.93.13 | controller-3、broker-3 |

| 位置 | 路径 |
| --- | --- |
| 本机管理配置 | 仓库下 `kafka/config/kafka-admin.conf` |
| 本机生成包 | 仓库下 `kafka/cluster/` |
| 节点 controller 包 | `/root/deploy/kafka/controller` |
| 节点 broker 包 | `/root/deploy/kafka/broker` |
| 节点 Kafka 数据 | `/data1/kafka/controller/metadata`、`/data1/kafka/broker/data` |
| 节点 Docker 数据 | `/data1/docker` |

生成器需要 Bash、Python 3、OpenSSL、base64 和 `sha256sum`，模拟节点需要 Docker 和 Compose。可在首次部署前执行以下命令检查环境。

```bash
docker info
docker compose version
python3 --version
command -v openssl
command -v base64
command -v sha256sum
```

## 2. 交互初始化

```bash
"$PWD/kafka/kafka-admin.sh" cluster init
"$PWD/kafka/kafka-admin.sh" cluster validate
```

按下表填写，其他提示可回车使用默认值。

| 提示 | 本地部署填写值 |
| --- | --- |
| 部署规格 | `manual` |
| 每个 controller 的 CPU | `0.5` |
| 每个 controller 的内存 | `512m` |
| 每个 broker 的 CPU | `1` |
| 每个 broker 的内存 | `1536m` |
| 三个 controller 地址 | `172.30.93.11,172.30.93.12,172.30.93.13` |
| 三个 broker 地址 | `172.30.93.11,172.30.93.12,172.30.93.13` |
| Kafka 数据基础目录 | `/data1/kafka` |
| 启用客户端认证 | `true` |
| 客户端认证端口 | `19092` |
| 初始账号 | `admin` |

首次密码留空会自动生成并保存，输入手动密码时终端不回显。已有认证配置留空沿用原密码。controller heap 自动为 256 MiB，broker heap 自动为 512 MiB。

六个角色包生成在仓库的 `kafka/cluster/nodes/`，认证连接配置在 `kafka/cluster/clients/admin.properties`。同一集群重复初始化会保留原身份。已有集群的账号密码通过在线账号管理修改，见 [authentication.md](authentication.md)。

## 3. 创建模拟节点并导入镜像

```bash
docker compose -f "$PWD/kafka/tests/local/docker-compose.yml" up -d --build
```

启动后逐条检查三个节点的内部 Docker。

```bash
docker exec hr-kafka-local-test-node-1-1 docker info --format 'name={{.Name}} root={{.DockerRootDir}} architecture={{.Architecture}}'
docker exec hr-kafka-local-test-node-2-1 docker info --format 'name={{.Name}} root={{.DockerRootDir}} architecture={{.Architecture}}'
docker exec hr-kafka-local-test-node-3-1 docker info --format 'name={{.Name}} root={{.DockerRootDir}} architecture={{.Architecture}}'
```

三条命令都应成功，且 `root=/data1/docker`。刚启动时可稍等几秒后重试。失败时执行下面的命令查看状态和原始日志，处理后再继续。

```bash
docker compose -f "$PWD/kafka/tests/local/docker-compose.yml" ps -a
docker compose -f "$PWD/kafka/tests/local/docker-compose.yml" logs --tail 50
```

拉取 Kafka 镜像并分别导入三个节点。本文使用 `apache/kafka:4.3.2`，如初始化时更换镜像，直接替换下文镜像地址。

```bash
docker pull apache/kafka:4.3.2
docker save apache/kafka:4.3.2 | docker exec -i hr-kafka-local-test-node-1-1 docker load
docker save apache/kafka:4.3.2 | docker exec -i hr-kafka-local-test-node-2-1 docker load
docker save apache/kafka:4.3.2 | docker exec -i hr-kafka-local-test-node-3-1 docker load
```

每个节点内部拥有独立的 Docker 镜像存储。命令成功后再执行下一步。

## 4. 分发六个角色包

在本机逐条执行。

```bash
docker exec hr-kafka-local-test-node-1-1 mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker
docker cp "$PWD/kafka/cluster/nodes/controller-1-172.30.93.11/." hr-kafka-local-test-node-1-1:/root/deploy/kafka/controller/
docker cp "$PWD/kafka/cluster/nodes/broker-1-172.30.93.11/." hr-kafka-local-test-node-1-1:/root/deploy/kafka/broker/

docker exec hr-kafka-local-test-node-2-1 mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker
docker cp "$PWD/kafka/cluster/nodes/controller-2-172.30.93.12/." hr-kafka-local-test-node-2-1:/root/deploy/kafka/controller/
docker cp "$PWD/kafka/cluster/nodes/broker-2-172.30.93.12/." hr-kafka-local-test-node-2-1:/root/deploy/kafka/broker/

docker exec hr-kafka-local-test-node-3-1 mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker
docker cp "$PWD/kafka/cluster/nodes/controller-3-172.30.93.13/." hr-kafka-local-test-node-3-1:/root/deploy/kafka/controller/
docker cp "$PWD/kafka/cluster/nodes/broker-3-172.30.93.13/." hr-kafka-local-test-node-3-1:/root/deploy/kafka/broker/
```

源路径末尾的 `/.` 会复制隐藏的 `.env` 文件。管理机的 `clients/admin.properties` 在验证时只交给选定节点。

## 5. 检查和首次格式化

在本机逐条执行，任一命令失败时先排查，再继续。

```bash
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/controller/deploy.sh check
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/controller/deploy.sh prepare
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/broker/deploy.sh check
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/broker/deploy.sh prepare

docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/controller/deploy.sh check
docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/controller/deploy.sh prepare
docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/broker/deploy.sh check
docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/broker/deploy.sh prepare

docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/controller/deploy.sh check
docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/controller/deploy.sh prepare
docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/broker/deploy.sh check
docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/broker/deploy.sh prepare
```

`prepare` 会创建数据目录并执行 KRaft 格式化。已有且身份一致的数据会跳过格式化，身份冲突或非空未格式化目录会报错。需要进入节点排查时执行 `docker exec -it hr-kafka-local-test-node-1-1 bash`。

## 6. 启动 controller，再启动 broker

先启动三个 controller。

```bash
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/controller/deploy.sh start
docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/controller/deploy.sh start
docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/controller/deploy.sh start
```

随后检查全部 controller 的集群身份、leader 和复制状态。

```bash
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/controller/deploy.sh health 120
```

健康检查成功后启动三个 broker。

```bash
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/broker/deploy.sh start
docker exec hr-kafka-local-test-node-2-1 bash /root/deploy/kafka/broker/deploy.sh start
docker exec hr-kafka-local-test-node-3-1 bash /root/deploy/kafka/broker/deploy.sh start
```

## 7. 验证消息收发

开启认证时按 [消息收发验证](verify-messages.md) 第 1–6 节操作。初始化时未开启认证，直接按该文档第 7 节进入模拟节点，通过 9092 端口执行集群检查和消息收发验证。

如需完整自动验收，在本机执行下面的命令。它还会检查资源、更新测试账号并重启和重建 broker。

```bash
bash "$PWD/kafka/tests/test-local.sh" verify &&
  python3 -m json.tool "$PWD/kafka/generated/local-test/result.json"
```

只有验收命令成功退出且本轮结果为 `status=passed` 才算通过。日志保存在仓库的 `kafka/generated/local-test/logs/`。记录执行时间、集群身份和本轮结果，按需归档。

## 8. 查看状态、日志与停止

在本机执行。

```bash
bash "$PWD/kafka/tests/test-local.sh" status
docker exec hr-kafka-local-test-node-1-1 bash /root/deploy/kafka/broker/deploy.sh logs
```

日志持续输出时按 Ctrl-C 退出。停止六个 Kafka 服务和三个模拟节点，保留测试盘。

```bash
bash "$PWD/kafka/tests/test-local.sh" stop
```

再次部署同一集群时先启动模拟节点，然后重新执行第 3 节的内部 Docker 检查、第 6 节启动和第 7 节验证。

```bash
docker compose -f "$PWD/kafka/tests/local/docker-compose.yml" start
```

## 9. 常见问题

- Docker 无法连接时，确认 Docker Desktop 已启动，当前终端的 Docker context 正确。
- 模拟节点未就绪时，执行第 3 节的状态和日志命令，查看原始错误。
- 网段冲突时，同步修改 Compose、测试启动脚本和验收脚本的地址，再重新生成包。
- 包摘要或数据身份不一致时，恢复匹配的完整节点包与配置。
- 从零重建需要清理旧数据盘和配置，先确认旧集群数据可以删除。
