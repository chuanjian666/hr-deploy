# 可选客户端账号认证

`cluster init` 支持选择是否启用账号认证，默认关闭。启用时使用 `SASL_PLAINTEXT + SCRAM-SHA-512`，不配置 TLS 或 ACL。broker 复制和 KRaft 控制面始终使用免认证的 `PLAINTEXT`。

| Listener | 默认端口 | 开启认证后的用途 |
| --- | ---: | --- |
| INTERNAL | 9092 | broker 复制及内部请求，仅允许集群节点互访 |
| CONTROLLER | 9093 | KRaft 控制面，仅允许集群节点互访 |
| EXTERNAL | 19092 | 所有业务客户端及管理工具，必须提供账号密码 |

EXTERNAL 表示客户端入口，同内网应用也连接该入口，不代表开放公网。启用认证时，即使 `ENABLE_EXTERNAL_LISTENER=false`，也会自动启用 EXTERNAL；关闭认证时仍由原开关控制是否启用额外的 PLAINTEXT 入口。

内部端口必须由防火墙或安全组限制为集群节点互访。否则客户端可通过 INTERNAL 绕过认证。消息传输不加密，应在可信隔离网络内使用。ACL 尚未启用，账号之间权限相同，`admin` 只是初始账号名称，不具有独占管理权限；后续可沿用账号配置 ACL。

## 初始化

交互执行 `./kafka-admin.sh cluster init` 会确认认证开关，开启时采集客户端端口和初始账号；首次创建时密码留空会自动生成，已有配置则沿用保存的密码，手动输入不回显。非交互初始化同样支持空密码自动生成。在 `/root/hr-deploy/kafka/config/kafka-admin.conf` 中填写以下配置：

```conf
ENABLE_AUTH="true"
AUTH_USERNAME="admin"
AUTH_PASSWORD="" # 首次 cluster init 自动生成并保存
EXTERNAL_PORT="19092"
```

账号名为 1–63 位 ASCII 字母、数字、点、下划线或连字符，首位必须为字母或数字。手动指定的初始密码不限制长度，但不能为空；支持 ASCII 字母、数字或 `_@%+=:./-`，以避免 Bash、Java properties 和 JAAS 多层转义歧义。自动生成使用 Python `secrets`，从大小写 ASCII 字母和数字中随机选择 6 个字符，保存到管理机配置，生成后的 `clients/admin.properties` 包含同一密码。随机密码不会打印到终端或初始化日志；重复初始化不会自动更换密码。单独执行 `config validate` 只校验、不生成密码；空密码在首次 `cluster init` 中补齐。配置文件权限应为 `600`；`config show` 会隐藏密码。优先使用交互输入或编辑配置文件，避免将真实密码写入 shell 历史。

生成产物：

- `cluster/clients/admin.properties`：管理工具可直接使用的连接配置，含明文密码，权限 `600`，仅留在管理机或交给授权客户端，不复制到全部节点。
- `cluster/bootstrap.scram`：带随机 salt、8192 次迭代的初始派生凭据，权限 `600`；也要作为敏感文件保管。
- controller 节点包的 `config/bootstrap.scram`：相同的派生凭据，纳入文件摘要校验，在首次 `prepare` 时通过 Kafka 自带 `kafka-storage.sh format --add-scram` 写入元数据。
- broker 节点包：仅含 SASL 服务配置，不分发初始账号明文密码或派生凭据。

若输出目录已有 manifest 或初始凭据，但管理机配置中的密码缺失，初始化会拒绝自动生成替代密码，需恢复原配置。三个 controller 必须使用同一批生成的节点包。重复初始化会复用 salt，保持初始凭据一致。若已有输出目录的初始账号或密码发生变化，生成会拒绝覆盖；现有账号应在线管理。新集群在独立的仓库副本中初始化，配置及节点包保存到该副本的默认目录。

生成节点包的管理机只需 Python 3 标准库（包括 `hashlib`、`base64`、`json` 等），无需 `pip install`。执行账号管理及消息验证的机器还需 Docker 和集群对应的 Kafka 镜像，无需在宿主机安装 Kafka CLI 或 Java。节点部署脚本不依赖 Python，继续使用 Bash、Docker、Compose 和常规 Linux 工具。

## Kafka CLI 执行方式和内存限制

本文及其他运维文档中的 Kafka 镜像内置工具统一通过独立的 `docker run --rm` 临时容器执行。不要在运行中的 broker 或 controller 容器里用 `docker exec` 启动 CLI JVM，以免挤占服务的内存额度。进入本地 DinD 模拟节点的 `docker exec` 只是进入 Linux 节点，Kafka CLI 仍在该节点内部通过 `docker run` 执行。

每个 CLI 容器使用以下资源配置，并逐条执行命令：

```bash
--memory 512m --memory-swap 512m \
-e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m'
```

`--memory 512m` 限制整个容器内存，`-Xmx256m` 限制 JVM 堆，剩余空间留给堆外内存、线程栈和 JVM 本身。`--memory-swap 512m` 与内存上限相同，表示不额外使用 swap，需要宿主机支持相应限制。每个临时容器都有自己的额度，多个并行命令会叠加消耗宿主机或模拟节点的资源；512 MiB 是日常管理和小规模验证的起点，不代表所有批量操作都能在该额度下运行。发生 OOM 时先检查执行规模和节点余量，再同时调整容器上限及 JVM 堆，并保留堆外余量。配置含义见 [Docker 内存限制](https://docs.docker.com/engine/containers/resource_constraints/#memory)。

下文默认镜像为 `apache/kafka:4.3.2`，如集群使用其他版本或内部镜像，应替换为集群对应的镜像；执行节点需提前拉取或离线导入。`--network host` 按 Linux 节点使用，所有 broker 的 advertised 地址都必须可达，不依赖本机 macOS 直接访问 DinD 网段。

## 不同生产者和消费者使用不同账号

### 1. 选择执行节点并准备管理连接

以下第 1–4 小节适用于已经开启认证的集群。未开启认证时使用后面的“未开启认证的本地集群”小节，不能直接修改初始化配置就认为现有集群已经开启认证。

本地模拟环境在本机终端执行，源文件使用当前仓库的绝对路径：

```bash
cd /path/to/hr-deploy
docker exec hr-kafka-local-test-node-1-1 install -d -m 700 /root/deploy/kafka/clients
docker cp "$PWD/kafka/cluster/clients/admin.properties" hr-kafka-local-test-node-1-1:/root/deploy/kafka/clients/admin.properties
docker exec -it hr-kafka-local-test-node-1-1 bash
```

进入节点后设置本地地址：

```bash
BOOTSTRAP=172.30.93.11:19092,172.30.93.12:19092,172.30.93.13:19092
```

生产环境在管理机执行以下命令，直接替换仓库路径和目标地址；只把管理连接配置交给选定的授权节点，不分发给全部服务器：

```bash
ssh root@10.0.0.11 'install -d -m 700 /root/deploy/kafka/clients'
scp /root/hr-deploy/kafka/cluster/clients/admin.properties root@10.0.0.11:/root/deploy/kafka/clients/admin.properties
ssh root@10.0.0.11
```

登录后设置生产地址：

```bash
BOOTSTRAP=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
```

两种环境后续命令一致，均在所选 Linux 节点的同一 Bash 会话执行：

```bash
chmod 600 /root/deploy/kafka/clients/admin.properties
```

如管理机本身是具备 Docker、镜像及网络访问能力的 Linux 机器，也可在管理机执行后续命令，将挂载源路径改为管理机上的实际路径。`admin.properties` 必须包含当前有效的密码；已在线轮换过密码时，旧生成文件可能失效。启用 ACL 后，该账号还必须具备管理权限。

### 2. 新建账号及修改密码

以下示例允许在命令中直接填写用户名和密码。将示例密码替换为各自实际密码；为了避免 Shell、SCRAM 参数和 JAAS 转义，本文示例使用非空 ASCII 字母、数字或 `_@%+=:./-`。账号规则与初始化相同。

```bash
# 创建生产者账号；对已有账号执行同一命令就是修改密码。
docker run --rm --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/admin.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server "$BOOTSTRAP" --command-config /run/admin.properties \
  --alter --entity-type users --entity-name hr_producer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=ProducerPassword_2026]'

# 创建消费者账号。
docker run --rm --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/admin.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server "$BOOTSTRAP" --command-config /run/admin.properties \
  --alter --entity-type users --entity-name hr_consumer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=ConsumerPassword_2026]'
```

`iterations=8192` 是生成 SCRAM 凭据的迭代次数，不是登录次数限制；Kafka 保存派生凭据。账号配置保存在集群元数据中，执行一次即可对整个集群生效，不用逐台 broker 创建，也不需要重启。

需要使用凭据文件时，可将对应命令的 `--add-config ...` 替换为 `--add-config-file /run/credential.properties`，并在镜像名称之前增加 `-v /root/deploy/kafka/clients/credential.properties:/run/credential.properties:ro`。宿主文件权限设为 600，内容为一行 `SCRAM-SHA-512=iterations=8192,password=<实际密码>`。

### 3. 列出账号、查询账号和删除凭据

```bash
# 列出集群中已配置 SCRAM 凭据的账号，不指定 --entity-name。
docker run --rm --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/admin.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server "$BOOTSTRAP" --command-config /run/admin.properties \
  --describe --entity-type users

# 查看单个账号，输出机制和迭代次数，不显示密码。
docker run --rm --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/admin.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server "$BOOTSTRAP" --command-config /run/admin.properties \
  --describe --entity-type users --entity-name hr_producer

# 仅在确认下线该账号时执行：删除它的 SCRAM-SHA-512 凭据。
docker run --rm --network host --user 0:0 \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  -v /root/deploy/kafka/clients/admin.properties:/run/admin.properties:ro \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server "$BOOTSTRAP" --command-config /run/admin.properties \
  --alter --entity-type users --entity-name hr_producer --delete-config SCRAM-SHA-512
```

修改密码或删除凭据作用于后续认证，不保证立即断开已有连接。删除凭据不会自动删除该用户名的 ACL，启用 ACL 的集群还应清理相应授权。列表显示的是 Kafka 中配置的凭据，不包括仅在 `super.users` 中声明、未创建 SCRAM 凭据的身份。

### 4. 应用使用账号

应用直接填写实际地址、用户名和密码。以下密码文字是需要替换的占位内容；本地模拟环境改为 `172.30.93.11`、`.12`、`.13`，且执行客户端的网络必须能访问这些地址：

```properties
bootstrap.servers=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="hr_producer" password="<生产者实际密码>";
```

消费者使用自己的账号和密码，并配置业务 `group.id`。同一服务的多个实例可共用账号；消费组名和认证账号分别配置，二者没有自动绑定关系。CLI 生产消费也通过带内存限制的 `docker run` 执行，见 [消息收发验证](verify-messages.md)；该文档默认使用管理连接文件，验证业务账号时将挂载源替换为该账号的客户端配置。不同账号的完整读写和权限拒绝示例见 [ACL 文档第 7 节](enable-acl-after-deploy.md#7-正向和反向验证)，其中权限拒绝部分要求已启用 ACL。

未配置 ACL 时，生产者、消费者账号均可读写及管理 Topic；后续启用 ACL 才能限制 Topic、consumer group 和管理操作。已有认证集群的启用顺序、账号授权、验证及回退见 [部署后启用 ACL 和账号授权](enable-acl-after-deploy.md)。

## 未开启认证的本地集群

适用于本地模拟节点仍使用 PLAINTEXT 9092 的情况。在本机终端进入节点，再执行下面的命令。无需管理连接配置，也不用进入运行中的 Kafka 服务容器：

```bash
docker exec -it hr-kafka-local-test-node-1-1 bash
```

进入后执行：

```bash
# 新建账号；账号已存在时会修改其密码。
docker run --rm --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server 172.30.93.11:9092,172.30.93.12:9092,172.30.93.13:9092 \
  --alter --entity-type users --entity-name hr_producer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=ProducerPassword_2026]'

# 列出账号；增加 --entity-name hr_producer 可只查一个账号。
docker run --rm --network host \
  --memory 512m --memory-swap 512m \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
  -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
  --entrypoint /opt/kafka/bin/kafka-configs.sh apache/kafka:4.3.2 \
  --bootstrap-server 172.30.93.11:9092,172.30.93.12:9092,172.30.93.13:9092 \
  --describe --entity-type users
```

这只是在集群元数据中保存账号，现有 9092 入口仍然不校验用户名和密码。要强制登录，需另行迁移服务端监听器和客户端配置；不要重新格式化现有 controller 来补账号，也不要将创建账号等同于开启认证。账号创建不等同于分配 ACL 权限。

## 验证

```bash
cd /root/hr-deploy/kafka
bash tests/test-basic.sh
python3 tests/test-auth.py
```

基础测试覆盖认证开关、随机密码生成与持久化、已有凭据缺失时拒绝替换、端口冲突、敏感配置权限及摘要、重复生成和关闭认证后的产物。认证测试需要本机 Docker 和 `apache/kafka:4.3.2` 镜像，使用独立网络的一台 controller 和三个 broker，验证初始认证、错误密码、无凭据连接拒绝、不同账号生产消费、三副本内部免认证复制、密码轮换及账号删除。测试结束自动清理容器和网络；此测试不替代完整三 controller 集群的部署验收。

Kafka 配置参考：[SASL/SCRAM 官方文档](https://kafka.apache.org/43/security/authentication-using-sasl/)。
