# 可选客户端账号认证

`cluster init` 支持选择是否启用账号认证，默认关闭以兼容旧配置。启用时使用 `SASL_PLAINTEXT + SCRAM-SHA-512`，不配置 TLS 或 ACL。broker 复制和 KRaft 控制面始终使用免认证的 `PLAINTEXT`。

| Listener | 默认端口 | 开启认证后的用途 |
| --- | ---: | --- |
| INTERNAL | 9092 | broker 复制及内部请求，仅允许集群节点互访 |
| CONTROLLER | 9093 | KRaft 控制面，仅允许集群节点互访 |
| EXTERNAL | 19092 | 所有业务客户端及管理工具，必须提供账号密码 |

EXTERNAL 表示客户端入口，同内网应用也连接该入口，不代表开放公网。启用认证时，即使 `ENABLE_EXTERNAL_LISTENER=false`，也会自动启用 EXTERNAL；关闭认证时仍由原开关控制是否启用额外的 PLAINTEXT 入口。

内部端口必须由防火墙或安全组限制为集群节点互访。否则客户端可通过 INTERNAL 绕过认证。消息传输不加密，应在可信隔离网络内使用。ACL 尚未启用，账号之间权限相同，`admin` 只是初始账号名称，不具有独占管理权限；后续可沿用账号配置 ACL。

## 初始化

交互执行 `./kafka-admin.sh cluster init` 会确认认证开关，开启时采集客户端端口、初始账号及密码，密码输入不回显。非交互方式编辑管理机配置：

```bash
ENABLE_AUTH="true"
AUTH_USERNAME="admin"
AUTH_PASSWORD="<替换为至少16位密码>"
EXTERNAL_PORT="19092"
```

账号名为 1–63 位 ASCII 字母、数字、点、下划线或连字符，首位必须为字母或数字。初始密码至少 16 位，支持 ASCII 字母、数字或 `_@%+=:./-`，以避免 Bash、Java properties 和 JAAS 多层转义歧义。示例中的占位符不能直接使用。配置文件权限应为 `600`；`config show` 会隐藏密码。优先使用交互输入或编辑配置文件，避免将真实密码写入 shell 历史。

生成产物：

- `cluster/clients/admin.properties`：管理工具可直接使用的连接配置，含明文密码，权限 `600`，仅留在管理机或交给授权客户端，不复制到全部节点。
- `cluster/bootstrap.scram`：带随机 salt、8192 次迭代的初始派生凭据，权限 `600`；也要作为敏感文件保管。
- controller 节点包的 `config/bootstrap.scram`：相同的派生凭据，纳入文件摘要校验，在首次 `prepare` 时通过 Kafka 自带 `kafka-storage.sh format --add-scram` 写入元数据。
- broker 节点包：仅含 SASL 服务配置，不分发初始账号明文密码或派生凭据。

三个 controller 必须使用同一批生成的节点包。重复初始化会复用 salt，保持初始凭据一致。若已有输出目录的初始账号或密码发生变化，生成会拒绝覆盖；现有账号应在线管理。尚未部署的全新集群可通过独立的 `KAFKA_CLUSTER_DIR` 重新生成。

管理机只需 Python 3 标准库（包括 `hashlib`、`base64`、`json` 等），无需 `pip install` 或额外 Kafka 客户端包。节点部署脚本不依赖 Python，继续使用 Bash、Docker、Compose 和常规 Linux 工具。

## 不同生产者和消费者使用不同账号

集群启动后可通过 Kafka 自带的 `kafka-configs.sh` 动态创建账号，无需重启 broker。以下命令在装有 Kafka CLI 的授权管理机运行，替换地址和示例密码；真实密码不要写入共享脚本或提交版本库。

```bash
KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
ADMIN_CONFIG=/secure/kafka/admin.properties

# 创建生产者账号；对已有账号执行同样命令即为修改密码。
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --alter \
  --entity-type users --entity-name hr_producer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=Replace-Producer-Password_2026]'

# 创建独立消费者账号。
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --alter \
  --entity-type users --entity-name hr_consumer \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=Replace-Consumer-Password_2026]'

# 查询账号凭据配置（不返回明文密码）。
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --describe --entity-type users --entity-name hr_producer

# 删除生产者账号的 SCRAM 凭据。
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --alter --entity-type users --entity-name hr_producer \
  --delete-config SCRAM-SHA-512
```

每个应用配置各自的用户名和密码，例如生产者：

```properties
bootstrap.servers=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="hr_producer" password="Replace-Producer-Password_2026";
```

消费者使用自己的账号，并配置业务 `group.id`。同一服务的多个实例可共用账号；消费组名和认证账号分别配置，二者没有自动绑定关系。凭据更新作用于后续认证，不保证立即断开已有连接。

未配置 ACL 时，生产者、消费者账号均可读写及管理 Topic；后续启用 ACL 才能限制 Topic、consumer group 和管理操作。

## 已有集群开启认证

`cluster init` 只生成配置，`prepare` 检测到已有且身份一致的数据目录后跳过格式化，因而不会自动创建或更新账号。

1. 先在受限的集群节点上，通过现有 INTERNAL 入口（9092）使用 `kafka-configs.sh` 创建初始 SCRAM 账号；此时不加 `--command-config`。
2. 使用相同账号密码生成认证配置并分发 broker 节点包，设置 19092 的访问规则，同时限制 9092/9093 的访问范围。
3. 逐台更新 broker。应用配置变更需要重建容器，应使用 `./deploy.sh stop` 后 `./deploy.sh start`；当前 `restart` 仅重启容器，不重新应用 Compose 配置。
4. 验证认证入口后，将客户端切换到 19092，逐一确认各 broker advertised 地址可达。

修改已有集群密码使用在线账号管理命令，并同步更新相应客户端配置。不得删除数据目录或重新格式化来应用账号变更。

## 验证

```bash
bash tests/test-basic.sh
python3 tests/test-auth.py
```

基础测试覆盖认证开关、旧配置兼容、缺失密码、端口冲突、敏感配置权限及摘要、重复生成和关闭认证后的产物。认证测试需要本机 Docker 和 `apache/kafka:4.3.2` 镜像，使用独立网络的一台 controller 和三个 broker，验证初始认证、错误密码、无凭据连接拒绝、不同账号生产消费、三副本内部免认证复制、密码轮换及账号删除。测试结束自动清理容器和网络；此测试不替代完整三 controller 集群的部署验收。

Kafka 配置参考：[SASL/SCRAM 官方文档](https://kafka.apache.org/43/security/authentication-using-sasl/)。
