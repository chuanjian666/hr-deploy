# 部署后启用 ACL 和账号授权

本文适用于已经部署、已开启 `SASL_PLAINTEXT + SCRAM-SHA-512` 客户端认证、尚未启用 ACL 的三 controller、三 broker KRaft 集群。首次启用需要更新六个角色的配置并逐节点重启，不需要重建集群或格式化数据；后续账号和 ACL 变更在线执行。

本文采用维护窗口切换：暂停业务生产和消费，启用默认拒绝策略，补齐所有业务授权并验证后恢复。未配置 authorizer 时不能先用 `kafka-acls.sh` 写入 ACL。本文不是无中断迁移方案，也不包含从无认证集群切换到 SCRAM 的步骤。

## 1. 执行位置、前提和权限规划

命令以 root 在 Linux 管理机运行，仓库路径为 `/root/hr-deploy`，节点为 `10.0.0.11`、`10.0.0.12`、`10.0.0.13`。执行前替换路径、地址、镜像和账号。管理机需要 Docker、Bash、Python 3、`sha256sum`、SSH 和 scp，并能访问三个 broker 的全部 advertised 地址；Kafka CLI 通过临时容器运行，不需要在宿主机安装 Kafka。

开始前确认：

- controller quorum 健康，三个 broker 已注册，没有离线分区，预期的三个副本均在 ISR；检查方法见 [消息收发验证](verify-messages.md) 第 3–4 节。
- 已取得原管理配置、完整节点包和 manifest，集群身份与服务器数据目录一致。本文不执行 `cluster init`、`prepare` 或任何 storage format 命令。
- `admin.properties` 中的账号密码当前有效；若已在线轮换密码，使用更新后的客户端配置，不能直接使用旧生成文件。
- 记录每个应用的账号、Topic、consumer group、是否使用幂等或事务生产，以及 Connect、Streams、监控工具等额外资源和操作需求。新规则必须覆盖全部现有业务，不能只授权本文的测试资源。
- 9092/9093 由防火墙或安全组限制为集群节点互访，业务及管理客户端只能连接认证端口 19092。

本文保留现有内部 PLAINTEXT 通信。其身份是 `User:ANONYMOUS`，会加入超级用户，以保障副本复制、broker 注册、controller quorum 及请求转发。**能够连接内部端口的任意客户端也会获得这个身份，并绕过 ACL。内部端口隔离是本方案的前提；ACL 不替代网络隔离。** 如需移除匿名超级用户，应另行将内部通信迁移到带认证的独立身份，不能直接删除它。

权限规划示例：

| 账号 | 资源 | 权限 |
| --- | --- | --- |
| `admin` | 全部 | 超级用户，管理账号、ACL 和 Topic |
| `hr_producer` | 指定 Topic | Write、Describe；按需补幂等及事务权限 |
| `hr_consumer` | 指定 Topic、指定消费组 | Topic Read/Describe、Group Read |
| `ANONYMOUS` | 内部通信 | 兼容当前免认证内部链路的超级用户，仅在网络隔离前提下使用 |

## 2. 备份并准备管理工具

**执行位置：管理机，同一个 Bash 会话。** 后续各节沿用本节变量和函数。新终端需恢复本次 `CHANGE_DIR` 的实际值，不要重新生成变更目录。

```bash
REPO=/root/hr-deploy/kafka
CHANGE_DIR="/root/kafka-acl-change-$(date +%Y%m%d-%H%M%S)"
CLIENT_DIR="$CHANGE_DIR/clients"
KAFKA_IMAGE=apache/kafka:4.3.2
BOOTSTRAP=10.0.0.11:19092,10.0.0.12:19092,10.0.0.13:19092
ACL_ADMIN=admin
ADMIN_CONFIG=/run/kafka-clients/admin.properties
umask 077
install -d -m 700 "$CHANGE_DIR/before/kafka" "$CLIENT_DIR"
cp -a "$REPO/config" "$REPO/cluster" "$REPO/templates" "$CHANGE_DIR/before/kafka/"
install -m 600 "$REPO/cluster/clients/admin.properties" "$CLIENT_DIR/admin.properties"

kafka_cli() {
  local script="$1"
  shift
  docker run --rm -i --network host --user 0:0 \
    --memory 512m --memory-swap 512m \
    -e KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' \
    -e KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml' \
    -v "$CLIENT_DIR:/run/kafka-clients:ro" \
    --entrypoint "/opt/kafka/bin/$script" "$KAFKA_IMAGE" "$@"
}

kafka_cli kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --describe --entity-type users
kafka_cli kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG"
```

如果管理密码已轮换，先通过编辑器修正 `$CLIENT_DIR/admin.properties` 再执行检查。`ACL_ADMIN` 必须与该文件实际使用的用户名一致。不要把密码写入命令参数或打印出来。

上面的 `kafka_cli` 将每个 Kafka 工具放在独立的临时容器中执行，容器内存上限 512 MiB、JVM 堆 128–256 MiB，不额外使用 swap。逐条运行命令，不在 broker/controller 服务容器中启动额外 JVM；资源说明见 [账号认证](authentication.md#kafka-cli-执行方式和内存限制)。

所有凭据都放在变更目录下，不依赖 `cluster/clients/` 保存新业务文件；`cluster init` 会整体替换生成目录。备份包含敏感信息，权限保持为 700/600，按凭据管理要求保存，不提交到 Git。另在每台服务器备份当前两个角色包，使用各自唯一的备份目录；包备份不能替代 Kafka 数据备份。

## 3. 准备业务账号和客户端配置

账号已存在时沿用现有密码和客户端文件，跳过对应的创建命令。对已有用户名执行 `--alter --add-config-file` 会轮换密码，不是无副作用的存在检查。

仅在需要创建示例账号时，在管理机用编辑器创建以下两个文件，并将占位文字替换为各自的实际密码：

```text
$CLIENT_DIR/producer-credential.properties
SCRAM-SHA-512=iterations=8192,password=<生产者实际密码>

$CLIENT_DIR/consumer-credential.properties
SCRAM-SHA-512=iterations=8192,password=<消费者实际密码>
```

每个文件只有一行 `SCRAM-SHA-512=...`，文件路径不属于内容。密码采用 ASCII 字母、数字或 `_@%+=:./-`，避免多层转义。创建后执行：

```bash
chmod 600 "$CLIENT_DIR/producer-credential.properties" "$CLIENT_DIR/consumer-credential.properties"
kafka_cli kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --alter --entity-type users --entity-name hr_producer \
  --add-config-file /run/kafka-clients/producer-credential.properties
kafka_cli kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --alter --entity-type users --entity-name hr_consumer \
  --add-config-file /run/kafka-clients/consumer-credential.properties
```

再用编辑器创建 `$CLIENT_DIR/producer.properties`：

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="hr_producer" password="<生产者实际密码>";
```

`$CLIENT_DIR/consumer.properties` 使用相同结构，将用户名改为 `hr_consumer`，填写其实际密码。两个客户端文件均设为 600。consumer group 后续单独指定，和认证账号没有自动绑定关系。

## 4. 暂停业务并离线更新六个节点包

暂停全部业务生产、消费和会执行 Kafka 管理操作的任务，确认维护窗口已经开始。保留管理账号的访问。默认拒绝配置生效后，未授权的业务请求会失败，直到第 6 节完成全部业务授权。

当前生成器没有 ACL 开关，清单中的 `security.aclEnabled` 固定生成为 `false`。以下命令直接更新管理机现有节点包，保留集群及节点身份，同时更新每个包的配置摘要、节点 manifest 以及集群 manifest。它不连接 Kafka、不修改账号和数据目录，也不直接修改服务器配置。

先检查原包完整性：

```bash
cd "$REPO"
./kafka-admin.sh cluster validate
```

检查通过后，执行以下完整代码块。`ACL_ADMIN` 沿用第 2 节实际管理用户名，不能把业务账号加入超级用户。

```bash
python3 - "$REPO/cluster" "$ACL_ADMIN" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
admin = sys.argv[2]
assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,62}', admin), '管理用户名不合法'
assert admin != 'ANONYMOUS', '请指定具名管理账号'
cluster = json.loads((root / 'manifest.json').read_text())
assert cluster['security']['authenticationEnabled'] is True, '本流程要求已开启认证'
nodes = sorted(p for p in (root / 'nodes').iterdir() if p.is_dir())
assert len(nodes) == 6, '应当有六个角色包'
roles = [json.loads((p / 'manifest.json').read_text())['node']['role'] for p in nodes]
assert roles.count('controller') == roles.count('broker') == 3, '角色数量不符合预期'
settings = {
    'authorizer.class.name': 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
    'super.users': f'User:{admin};User:ANONYMOUS',
    'allow.everyone.if.no.acl.found': 'false',
}
updates = []
for node in nodes:
    props = node / 'config/server.properties'
    lines = props.read_text().splitlines()
    current = {}
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith(('#', '!')) and '=' in stripped:
            key, value = stripped.split('=', 1)
            current[key.strip()] = value.strip()
    for key, value in settings.items():
        assert key not in current or current[key] == value, f'{node.name}: 已有不同的 {key}，停止并人工核对'
    kept = [line for line in lines if line.split('=', 1)[0].strip() not in settings]
    content = '\n'.join(kept + [f'{key}={value}' for key, value in settings.items()]) + '\n'
    digest = hashlib.sha256(content.encode()).hexdigest()
    checksum_path = node / 'checksums.sha256'
    checksum_lines = checksum_path.read_text().splitlines()
    assert sum(line.split(maxsplit=1)[-1] == 'config/server.properties' for line in checksum_lines) == 1
    checksums = '\n'.join(
        f'{digest}  config/server.properties' if line.split(maxsplit=1)[-1] == 'config/server.properties' else line
        for line in checksum_lines
    ) + '\n'
    manifest = json.loads((node / 'manifest.json').read_text())
    manifest['checksums']['serverProperties'] = digest
    updates.append((props, content, checksum_path, checksums, node / 'manifest.json', manifest))
for props, content, checksum_path, checksums, manifest_path, manifest in updates:
    props.write_text(content)
    checksum_path.write_text(checksums)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
cluster['security']['aclEnabled'] = True
(root / 'manifest.json').write_text(json.dumps(cluster, ensure_ascii=False, indent=2) + '\n')
print('已更新六个节点包、摘要和清单；尚未在运行集群启用 ACL')
PY

./kafka-admin.sh cluster validate
```

核对变更仅包含三项 ACL 配置、对应的摘要和清单标记；cluster ID、node ID、controller directory ID、地址、端口、镜像及数据路径必须与备份相同。`aclEnabled=true` 是部署意图记录，不能证明运行配置已经生效。静态校验也不代表 ACL 功能通过验收。

这三项配置会写入全部 controller 和 broker：

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:ANONYMOUS
allow.everyone.if.no.acl.found=false
```

无匹配 ACL 的业务访问将被拒绝，超级用户绕过 ACL。不要使用 `allow.everyone.if.no.acl.found=true` 当作最终配置；它允许访问没有任何 ACL 的资源，且给某资源加第一条 ACL 时就可能改变其他用户对该资源的访问行为。

**以后再次执行 `cluster init` 会按模板重新生成配置，并将 ACL 标记重置为 `false`。在生成器原生支持 ACL 前，每次重新生成都必须在分发前重新执行本节更新、校验及差异检查。不要分发未补齐 ACL 的新生成包。** 如团队维护了带 ACL 的模板，仍需核对生成清单及全部节点的实际配置。

## 5. 分发节点包并逐角色切换

**执行位置：管理机。** 对一个角色完成“复制包、检查、stop/start、恢复验证”后，再操作下一个角色。以下示例用于第一个 controller：

```bash
scp -r "$REPO/cluster/nodes/controller-1-10.0.0.11/." root@10.0.0.11:/root/deploy/kafka/controller/
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh check'
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh stop'
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh start'
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh health 120'
```

依次将包名改为 `controller-2-10.0.0.12`、`controller-3-10.0.0.13`，SSH 目标改为对应地址，服务器目录均为 `/root/deploy/kafka/controller/`。可以先操作 follower、最后操作当前 leader；无论顺序如何，同一时间只能有一个 controller 不可用。每次 `health 120` 通过后再继续；失败时停止后续切换，查看该角色日志。

三个 controller 完成后，按相同原则逐台操作 broker。第一个 broker 示例：

```bash
scp -r "$REPO/cluster/nodes/broker-1-10.0.0.11/." root@10.0.0.11:/root/deploy/kafka/broker/
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh check'
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh stop'
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh start'
kafka_cli kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG"
kafka_cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --describe
```

确认该 broker 重新注册，业务分区均有合法 leader，期望的三个副本全部回到 ISR 后，再复制并切换 `broker-2-10.0.0.12`、`broker-3-10.0.0.13`。`status` 或容器 Running 不代表恢复完成。

这里使用 `stop` / `start` 重建容器，以应用完整节点包；`stop` 不删除宿主数据。不要执行 `prepare`，不要删除 `meta.properties` 或数据目录，不要同时停多个同角色节点。管理账号凭据始终留在管理机，不随包分发。

全部六个角色恢复后，执行以下命令并保存结果；成功列出 ACL（首次可为空）说明 authorizer 管理接口已可用：

```bash
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" --list > "$CHANGE_DIR/acls-before.txt"
```

## 6. 授权 Topic、消费组和事务

以下先用专用测试资源验证权限。`acl-check-manual` 和 `acl-check-denied-manual` 必须是本轮未使用过的名称；已有同名资源时，先选新名称并统一替换，不删除既有资源来腾出名称。生产业务按第 1 节权限矩阵重复授权实际资源，完成后才能恢复。

```bash
TOPIC=acl-check-manual
GROUP=acl-check-manual-group
DENIED_TOPIC=acl-check-denied-manual
kafka_cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2
kafka_cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --topic "$DENIED_TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2

# 生产者只允许写入和查看指定 Topic，不授予创建或删除 Topic 的权限。
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:hr_producer \
  --operation Write --operation Describe --topic "$TOPIC"

# 按需：幂等生产的集群级权限，不授予其他 Topic 的 Write。
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:hr_producer --operation IdempotentWrite --cluster

# 消费权限必须同时覆盖 Topic 和实际 group.id。
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:hr_consumer --consumer --topic "$TOPIC" --group "$GROUP"
```

`--consumer` 添加 Topic Read/Describe 和 Group Read。常见的 `--producer` 快捷选项还会授予 Topic Create；上面显式列出操作以避免附带创建权限。

只有应用使用事务生产时，再按实际 `transactional.id` 授权；以下 `hr-events-tx-1` 是需替换的示例值：

```bash
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:hr_producer \
  --operation Write --operation Describe --transactional-id hr-events-tx-1
```

事务不替代 Topic Write；若应用在事务中提交消费位点，还需为实际执行该事务的账号授予相应 Topic 和 Group 权限。多个事务实例、Streams 或 Connect 需要按实际内部 Topic、group 和事务 ID 另列授权。需要前缀匹配时使用 `--resource-pattern-type prefixed` 并核对前缀范围；默认是 literal，不会自动匹配同名前缀。不要为了消除报错直接放行所有 Topic 或把应用加入超级用户。

查询所有对某资源生效的规则，包括通配和前缀规则：

```bash
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --list --topic "$TOPIC" --resource-pattern-type match
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --list --group "$GROUP" --resource-pattern-type match
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --list > "$CHANGE_DIR/acls-after.txt"
```

ACL 保存在 KRaft 元数据中，通过任一 broker 管理一次即可，不用逐台创建；后续增删规则不需要重启。

## 7. 正向和反向验证

先确认两个测试 Topic 的 ISR 均包含三个副本，然后用第 3 节客户端文件验证。认证配置中的密码必须与实际账号一致。

```bash
chmod 600 "$CLIENT_DIR/producer.properties" "$CLIENT_DIR/consumer.properties"
printf '%s\n' acl-check-1 acl-check-2 acl-check-3 > "$CHANGE_DIR/expected.txt"
kafka_cli kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /run/kafka-clients/producer.properties --topic "$TOPIC" \
  --producer-property acks=all < "$CHANGE_DIR/expected.txt"
kafka_cli kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /run/kafka-clients/consumer.properties --topic "$TOPIC" \
  --group "$GROUP" --from-beginning --max-messages 3 --timeout-ms 30000 \
  > "$CHANGE_DIR/actual.txt"
diff -u "$CHANGE_DIR/expected.txt" "$CHANGE_DIR/actual.txt"
```

`diff` 必须成功且无差异。下面三条命令是预期失败的反向验证，逐条执行并检查日志。不要只看 console 工具或 `tee` 的退出码：应明确看到 Topic 或 Group 的授权错误；网络超时、认证失败不能作为 ACL 生效证据。

```bash
# 消费者不能向测试 Topic 写入。
printf '%s\n' denied-write | kafka_cli kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /run/kafka-clients/consumer.properties --topic "$TOPIC" \
  --producer-property enable.idempotence=false --producer-property max.block.ms=10000 \
  --producer-property delivery.timeout.ms=10000 --producer-property request.timeout.ms=5000 \
  2>&1 | tee "$CHANGE_DIR/deny-consumer-write.log"

# 生产者不能访问未授权的另一个 Topic。
printf '%s\n' denied-topic | kafka_cli kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /run/kafka-clients/producer.properties --topic "$DENIED_TOPIC" \
  --producer-property max.block.ms=10000 --producer-property delivery.timeout.ms=10000 \
  --producer-property request.timeout.ms=5000 2>&1 | tee "$CHANGE_DIR/deny-other-topic.log"

# 消费者不能换用未授权的消费组。
kafka_cli kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /run/kafka-clients/consumer.properties --topic "$TOPIC" \
  --group acl-check-ungranted-group --from-beginning --max-messages 1 --timeout-ms 10000 \
  2>&1 | tee "$CHANGE_DIR/deny-other-group.log"
```

若账号已有其他规则，上述“未授权”资源可能被旧通配或前缀规则允许，应先用 `--list` 核对并选择确实未授权的资源，不能据此盲目移除业务 ACL。反向验证出现意外成功时，保持业务暂停，检查是否还有未启用 authorizer 的节点、是否误用了管理配置或内部端口、账号是否为超级用户，以及是否存在更宽的 Allow。

最后确认六个节点均加载新配置，controller 健康，broker 注册及全量业务分区 ISR 正常；从实际应用网络检查全部 advertised 地址，并验证实际业务的 Topic/group/事务需求。完成全部业务授权及正反向验证后才恢复生产、消费和管理任务，观察应用授权错误和消费积压，归档变更包、ACL 列表、日志及读写证据。

## 8. 日常账号授权和撤销

新账号按第 3 节创建，再按第 6 节授权。重设密码不改变已有 ACL；删除凭据也不会清除对应用户名的 ACL。日后复用同名账号可能重新获得遗留权限，因此账号下线要同时清理凭据及其所有授权。

以下仅撤销示例 Topic 的两项生产权限。确认操作对象后按交互提示执行，不使用模糊的批量删除：

```bash
kafka_cli kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --remove --allow-principal User:hr_producer \
  --operation Write --operation Describe --topic "$TOPIC"
```

消费权限可使用 `--remove --allow-principal User:hr_consumer --consumer --topic "$TOPIC" --group "$GROUP"`，其他 Topic、消费组、幂等及事务授权分别核对撤销。集群级幂等权限可能被该账号的其他业务共用，不随一个 Topic 下线盲目删除。删除账号 SCRAM 凭据的方法见 [账号认证](authentication.md)；凭据删除不保证立即断开已经认证的连接。

测试资源和规则按需清理，操作前确认确属本轮测试且未被业务使用。长期保留的业务授权应记录账号、资源、操作、用途和负责人；含明文密码的临时文件按本地凭据保管要求清理或转存。

## 9. 失败处理和回退

| 现象 | 优先检查 |
| --- | --- |
| `No Authorizer is configured` / `SecurityDisabledException` | controller 和 broker 是否都配置并加载了 StandardAuthorizer；清单标记不代表实际启用 |
| 管理账号被拒绝 | 客户端实际用户名、`User:` 大小写及六个节点的 `super.users`；超级用户用分号分隔 |
| broker 注册、复制或 controller 通信授权失败 | 当前 PLAINTEXT 身份 `User:ANONYMOUS` 是否在所有节点得到保留 |
| `TopicAuthorizationException` | 账号实际使用的 Topic、操作、前缀规则及是否存在 Deny |
| `GroupAuthorizationException` | 实际 `group.id` 是否授权，是否误把账号名当消费组名 |
| `TransactionalIdAuthorizationException` | 实际事务 ID 和账号的 Write/Describe 权限 |
| 节点包摘要不匹配 | 是否分发完整包，是否手改配置后没有同步摘要；不要直接跳过校验 |

部分节点切换失败时停止后续操作，保持业务暂停。先修复当前节点或回退该节点，恢复 quorum 和 ISR；不要继续停下一个节点。

需要完整回退时，恢复第 2 节保存的**原完整节点包**，按第 5 节相同的一次一个角色、恢复后再继续的原则分发并 `stop` / `start`。例如回退第一个 broker：

```bash
scp -r "$CHANGE_DIR/before/kafka/cluster/nodes/broker-1-10.0.0.11/." \
  root@10.0.0.11:/root/deploy/kafka/broker/
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh check'
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh stop'
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh start'
```

恢复 broker 注册及 ISR 后再处理下一个；controller 同理，恢复后执行 `health 120`。保留当前更新包用于排查，并把管理机 `cluster/` 恢复为与最终运行版本一致的原包。若启用期间也修改过模板，应同时恢复与部署版本匹配的模板。仅改 manifest 或 `allow.everyone.if.no.acl.found` 不是完整回退。

去掉 authorizer 后仍保留客户端 SCRAM 认证，但账号再次拥有不受 ACL 限制的访问能力；网络隔离继续保留。已经创建的账号、密码及 KRaft ACL 记录不会因回退节点配置自动撤销。重新启用 ACL 时必须检查并复用或调整这些规则；若要清理，需在 authorizer 可用时由管理账号明确操作。不要通过清空元数据来回退。

## 10. 参考及验证边界

- [Kafka 4.3 ACL 配置、命令和权限映射](https://kafka.apache.org/43/security/authorization-and-acls/)：KRaft 的 authorizer、超级用户、资源及授权管理。
- [Kafka 4.3 身份与监听器配置](https://kafka.apache.org/43/generated/kafka_config.html)：PLAINTEXT 使用 ANONYMOUS 身份。
- [在运行集群引入安全特性](https://kafka.apache.org/43/security/incorporating-security-features-in-a-running-cluster/)：安全配置迁移的运行考虑。
- [账号认证](authentication.md)、[生产部署](deploy-production.md)、[消息收发验证](verify-messages.md)。

本文给出人工运维流程，不表示仓库已有原生 ACL 配置开关。现有认证自动化测试不覆盖本流程；`cluster validate` 只验证静态包，三 controller / 三 broker 的真实启用、拒绝行为和回退需在目标测试环境演练并在上线时留档。
