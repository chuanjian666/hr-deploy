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

管理机只需 Python 3 标准库（包括 `hashlib`、`base64`、`json` 等），无需 `pip install` 或额外 Kafka 客户端包。节点部署脚本不依赖 Python，继续使用 Bash、Docker、Compose 和常规 Linux 工具。

## 不同生产者和消费者使用不同账号

集群启动后可通过 Kafka 自带的 `kafka-configs.sh` 动态创建账号。以下命令在安装了 Kafka CLI 的授权管理机运行，CLI 目录以 `/opt/kafka/bin` 为例，broker 地址以 `10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092` 为例。执行前直接替换示例路径和地址。

管理配置使用 `/root/hr-deploy/kafka/cluster/clients/admin.properties`。在 `/root/hr-deploy/kafka/cluster/clients/producer-credential.properties` 和 `consumer-credential.properties` 中分别填写 `SCRAM-SHA-512=iterations=8192,password=<实际密码>`，将占位文字替换为各自密码，文件权限设为 `600`。

```bash
chmod 600 /root/hr-deploy/kafka/cluster/clients/producer-credential.properties
chmod 600 /root/hr-deploy/kafka/cluster/clients/consumer-credential.properties

# 创建生产者账号，对已有账号执行即为修改密码。
/opt/kafka/bin/kafka-configs.sh --bootstrap-server 10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092 \
  --command-config /root/hr-deploy/kafka/cluster/clients/admin.properties --alter \
  --entity-type users --entity-name hr_producer \
  --add-config-file /root/hr-deploy/kafka/cluster/clients/producer-credential.properties

# 创建消费者账号。
/opt/kafka/bin/kafka-configs.sh --bootstrap-server 10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092 \
  --command-config /root/hr-deploy/kafka/cluster/clients/admin.properties --alter \
  --entity-type users --entity-name hr_consumer \
  --add-config-file /root/hr-deploy/kafka/cluster/clients/consumer-credential.properties

# 查询账号凭据配置。
/opt/kafka/bin/kafka-configs.sh --bootstrap-server 10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092 \
  --command-config /root/hr-deploy/kafka/cluster/clients/admin.properties \
  --describe --entity-type users --entity-name hr_producer

# 按需删除生产者账号的 SCRAM 凭据。
/opt/kafka/bin/kafka-configs.sh --bootstrap-server 10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092 \
  --command-config /root/hr-deploy/kafka/cluster/clients/admin.properties \
  --alter --entity-type users --entity-name hr_producer --delete-config SCRAM-SHA-512
```

应用直接填写实际连接地址、用户名和密码。以下密码文字是需要替换的占位内容：

```properties
bootstrap.servers=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="hr_producer" password="<生产者实际密码>";
```

消费者使用自己的账号，并配置业务 `group.id`。同一服务的多个实例可共用账号；消费组名和认证账号分别配置，二者没有自动绑定关系。凭据更新作用于后续认证，不保证立即断开已有连接。

未配置 ACL 时，生产者、消费者账号均可读写及管理 Topic；后续启用 ACL 才能限制 Topic、consumer group 和管理操作。

## 验证

```bash
cd /root/hr-deploy/kafka
bash tests/test-basic.sh
python3 tests/test-auth.py
```

基础测试覆盖认证开关、随机密码生成与持久化、已有凭据缺失时拒绝替换、端口冲突、敏感配置权限及摘要、重复生成和关闭认证后的产物。认证测试需要本机 Docker 和 `apache/kafka:4.3.2` 镜像，使用独立网络的一台 controller 和三个 broker，验证初始认证、错误密码、无凭据连接拒绝、不同账号生产消费、三副本内部免认证复制、密码轮换及账号删除。测试结束自动清理容器和网络；此测试不替代完整三 controller 集群的部署验收。

Kafka 配置参考：[SASL/SCRAM 官方文档](https://kafka.apache.org/43/security/authentication-using-sasl/)。
