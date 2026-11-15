# Kafka 部署后消息收发验证

本地 DinD 和生产服务器使用同一套 Kafka CLI 命令。执行位置是任一 broker 所在的 Linux 节点；两种环境只在进入节点和复制连接配置时不同。验证会创建专用 Topic、消费组并发送三条消息，不修改账号、不重启服务。

本文是手动执行的验证文档，按顺序在终端操作。前提是三个 controller 和三个 broker 已完成部署，controller 的 `health` 检查通过。

第 1–6 节用于已开启认证的集群。初始化时未开启认证的集群直接执行第 7 节。

通过条件：

- 三个 broker 均已注册，controller quorum 和复制正常。
- 测试 Topic 的分区有合法 leader，三个副本均进入 ISR。
- 以 `acks=all` 发送三条消息成功，消费得到三条消息，且内容比对无差异。

## 1. 准备连接配置并进入节点

启用认证时，管理机生成的连接配置位于仓库的 `kafka/cluster/clients/admin.properties`。本文生产管理机仓库路径以 `/root/hr-deploy` 为例，验证服务器以 `10.0.0.11` 为例，执行前直接替换为实际路径和地址。

### 测试环境入口

在本机终端进入仓库根目录，以下以第一个模拟节点为验证入口。

```bash
cd /path/to/hr-deploy
docker exec hr-kafka-local-test-node-1-1 install -d -m 700 /root/deploy/kafka/clients
docker cp "$PWD/kafka/cluster/clients/admin.properties" hr-kafka-local-test-node-1-1:/root/deploy/kafka/clients/admin.properties
docker exec -it hr-kafka-local-test-node-1-1 bash
```

后续命令在进入后的节点内执行。

### 生产入口

在管理机执行：

```bash
ssh root@10.0.0.11 'install -d -m 700 /root/deploy/kafka/clients'
scp /root/hr-deploy/kafka/cluster/clients/admin.properties root@10.0.0.11:/root/deploy/kafka/clients/admin.properties
ssh root@10.0.0.11
```

后续命令在选定的 broker 节点执行。

## 2. 准备验证目录

本地和生产节点使用下文相同的命令。本文镜像为 `apache/kafka:4.3.2`，如初始化时使用其他镜像，直接替换下文镜像地址。客户端从本机 `127.0.0.1:19092` 连接 broker，集群返回的三个 advertised 地址也必须可达。若客户端端口不同，替换下文端口。

```bash
chmod 600 /root/deploy/kafka/clients/admin.properties
install -d -m 700 /root/deploy/kafka/message-check
```

每条 Kafka 命令使用独立的 `docker run --rm` 临时 CLI 容器，并在结束后自动删除。每个容器内存上限为 512 MiB，JVM 堆为 128–256 MiB，`--memory-swap 512m` 禁止额外使用 swap；连接配置以只读方式挂载。逐条执行，避免多个 JVM 同时挤占节点资源。任一命令失败时先排查，再继续下一步，资源调整方法见 [账号认证](authentication.md#kafka-cli-执行方式和内存限制)。

本节使用已启用认证的集群，免认证操作见第 7 节。

## 3. 检查集群状态

```bash
docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-broker-api-versions.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties

docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-metadata-quorum.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties describe --status

docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-metadata-quorum.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties describe --replication
```

确认 broker ID 为 1/2/3，controller voter 为 101/102/103，有合法 leader，复制 lag 为 0。

## 4. 创建专用 Topic

示例 Topic 为 `message-check-manual`，consumer group 为 `message-check-manual-consumer`。每轮验证前选用新的专用名称，并直接替换下文全部相同名称，以便从头消费本轮三条消息。若创建时提示 Topic 已存在，先更换名称。

使用一个分区、三个副本、min ISR 2：

```bash
docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties --create --topic message-check-manual --partitions 1 --replication-factor 3 --config min.insync.replicas=2

docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties --describe --topic message-check-manual
```

继续前确认分区有合法 leader，Replicas 和 ISR 都包含 broker 1/2/3。未满足时等待并重新执行 describe。

## 5. 发送、接收并比对消息

在同一个验证节点逐条执行，每条命令成功后再执行下一条：

```bash
printf '%s\n' hello-kafka-1 hello-kafka-2 hello-kafka-3 > /root/deploy/kafka/message-check/expected.txt

docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-console-producer.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties --topic message-check-manual --producer-property acks=all < /root/deploy/kafka/message-check/expected.txt

docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-console-consumer.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties --topic message-check-manual --group message-check-manual-consumer --from-beginning --max-messages 3 --timeout-ms 30000 > /root/deploy/kafka/message-check/actual.txt

cat /root/deploy/kafka/message-check/actual.txt
diff -u /root/deploy/kafka/message-check/expected.txt /root/deploy/kafka/message-check/actual.txt
```

消费结果包含三条消息，且 `diff` 成功退出、无差异输出，才表示本轮收发通过。证据保存在 `/root/deploy/kafka/message-check/`，按执行时间另行归档。

本轮检查所选节点到 Kafka 的链路和三副本读写。生产部署还需从应用所在网络检查三个 advertised 地址及应用账号，并按部署方案完成容量和故障恢复验收。

## 6. 按需清理

确认本轮验收完成后，可删除本轮 Topic 和验证节点上的连接配置：

```bash
docker run --rm -i --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/kafka-client.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:19092 --command-config /run/kafka-client.properties --delete --topic message-check-manual

rm -f /root/deploy/kafka/clients/admin.properties
```

删除 Topic 时使用本轮实际名称。连接配置删除后，后续验证需重新从管理机复制。


## 7. 未开启认证的消息收发

适用于初始化时关闭账号认证的集群。以下通过默认的 PLAINTEXT 端口 9092 连接节点上的 broker，镜像采用 `apache/kafka:4.3.2`。如果初始化时修改过端口或镜像，直接替换命令中的值。三个 controller 和三个 broker 应已启动，controller 的健康检查已通过。

### 进入验证节点

本地模拟环境在本机终端执行：

```bash
docker exec -it hr-kafka-local-test-node-1-1 bash
```

真实服务器在管理机执行，将示例 IP 替换为实际地址：

```bash
ssh root@10.0.0.11
```

后续所有命令在进入后的 broker 节点执行。CLI 同样通过独立临时容器运行，内存上限 512 MiB、JVM 堆 128–256 MiB，命令逐条执行。本机入口 `127.0.0.1:9092` 使用 host 网络连接 broker，三个 advertised 地址也必须在该节点内可达。

### 检查 broker 和 controller

```bash
docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-broker-api-versions.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092

docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-metadata-quorum.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 describe --status

docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-metadata-quorum.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 describe --replication
```

确认 broker ID 为 1/2/3，controller voter 为 101/102/103，有合法 leader，复制 lag 为 0。每条命令成功后再继续。

### 创建 Topic 并确认副本

示例 Topic 为 `message-check-noauth`，消费组为 `message-check-noauth-consumer`。每轮验收使用新的专用 Topic 和消费组名称，直接替换下面的相同名称。Topic 已存在时先更换名称再创建。

```bash
docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 --create --topic message-check-noauth --partitions 1 --replication-factor 3 --config min.insync.replicas=2

docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 --describe --topic message-check-noauth
```

确认分区有合法 leader，Replicas 和 ISR 都包含 broker 1/2/3。未满足时等待并重新执行 describe。

### 发送三条消息

```bash
install -d -m 700 /root/deploy/kafka/message-check-noauth
printf '%s\n' hello-kafka-1 hello-kafka-2 hello-kafka-3 > /root/deploy/kafka/message-check-noauth/expected.txt

docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-console-producer.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 --topic message-check-noauth --producer-property acks=all < /root/deploy/kafka/message-check-noauth/expected.txt
```

### 消费并比对消息

```bash
docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-console-consumer.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 --topic message-check-noauth --group message-check-noauth-consumer --from-beginning --max-messages 3 --timeout-ms 30000 > /root/deploy/kafka/message-check-noauth/actual.txt

cat /root/deploy/kafka/message-check-noauth/actual.txt
diff -u /root/deploy/kafka/message-check-noauth/expected.txt /root/deploy/kafka/message-check-noauth/actual.txt
```

消费输出应包含三条消息，`diff` 成功退出且无差异输出表示本轮收发通过。证据保存在 `/root/deploy/kafka/message-check-noauth/`，按本轮执行时间归档。

### 按需清理 Topic

确认验收完成后，使用本轮实际 Topic 名称执行：

```bash
docker run --rm -i --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 \
  --bootstrap-server 127.0.0.1:9092 --delete --topic message-check-noauth
```
