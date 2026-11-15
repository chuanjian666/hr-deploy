# Kafka 生产部署指南

本文依据当前仓库脚本，说明在三台 Linux 服务器上首次部署 3 个 controller 和 3 个 broker 的过程。每台服务器共置一个 controller 和一个 broker，角色使用独立的 Docker Compose 包。本文不启动本地 DinD 模拟节点，也不调用 `tests/test-local.sh verify`。

本文是供部署人员使用的通用操作手册，不记录具体环境或历史验收结论。上线验收应在目标环境执行并另行留档，认证配置和账号管理见 [authentication.md](authentication.md)，本地演练见 [deploy-local.md](deploy-local.md)。

## 1. 确认拓扑、资源和网络

本文统一以仓库路径 `/root/hr-deploy` 和下表地址为示例。执行前直接将命令中的示例路径、IP 和镜像替换成实际值。地址必须稳定且所有客户端可达；本文命令采用 root SSH，若使用普通运维账号，需按实际 sudo 和目录权限调整。

| 服务器 | 示例地址 | Controller ID | Broker ID |
| --- | --- | --- | --- |
| node-1 | 10.0.0.11 | 101 | 1 |
| node-2 | 10.0.0.12 | 102 | 2 |
| node-3 | 10.0.0.13 | 103 | 3 |

| 规格 | 每台服务器 | Controller / heap | Broker / heap |
| --- | --- | --- | --- |
| standard（本文采用） | 8 CPU / 32 GiB | 1 CPU、2 GiB / 1 GiB | 6 CPU、24 GiB / 8 GiB |
| minimal | 4 CPU / 16 GiB | 1 CPU、2 GiB / 1 GiB | 2 CPU、10 GiB / 4 GiB |

两档均不限定磁盘容量或介质，容量按实际业务规划；Kafka 数据盘需持久、独立挂载，节点脚本根据 `KAFKA_DATA_BASE_DIR` 自动识别所在文件系统，无需另填挂载点。minimal 需要按真实业务压测；`manual` 不用于生产。保留期默认 168 小时，没有总数据量上限，需结合流量、副本数、保留期和磁盘余量评估容量。

本文采用默认目录布局；Kafka 数据根目录和 Docker 存储目录可分别配置，部署包目录仍固定：

```text
/data1/docker                        Docker data-root
/data1/kafka/controller/metadata  controller 元数据
/data1/kafka/broker/data          broker 数据
/root/deploy/kafka/controller     controller 部署包
/root/deploy/kafka/broker         broker 部署包
```

自定义路径时，直接修改第 2 节命令中的数据盘路径和 Docker 的 `--data-root` 参数，第 3 节在交互提示中填写实际 Kafka 数据基础目录。各节点的 Kafka 数据目录采用相同路径。

本文启用客户端 SCRAM 认证。当前实现没有 TLS 和 ACL，仅适用于已接受此安全边界的可信隔离内网；账号认证不提供 Topic 权限隔离。

| 端口 | 放行范围 | 用途 |
| --- | --- | --- |
| TCP 9093 | 集群节点互访 | KRaft 控制面 |
| TCP 9092 | 集群节点互访 | 内部通信，免认证 |
| TCP 19092 | 业务客户端、授权管理机到三个 broker | SASL_PLAINTEXT + SCRAM-SHA-512 |
| TCP 22 | 运维管理机到服务器 | SSH / scp |

限制 9092 的访问范围是认证生效的必要条件，否则客户端可通过内部端口绕过认证。所有业务客户端必须能访问三个 broker 的 advertised 地址；只放通一个 bootstrap 地址不够。本文使用相同的内外地址，不包含 NAT、端口转发或负载均衡适配。

## 2. 准备三台 Linux 服务器

**执行位置：每台目标服务器，root。** 操作前按实际设备完成数据盘格式化、持久挂载和时间同步。本文不提供通用格式化命令，避免对错误设备操作。

```bash
mountpoint /data1
findmnt /data1
df -h /data1
```

确认 `/data1` 是预期的独立数据盘，且重启后仍挂载。将仓库的 `docker/` 目录复制到每台服务器的 `/root/hr-deploy/docker/`，然后执行：

```bash
cd /root/hr-deploy/docker
bash /root/hr-deploy/docker/setup-docker.sh --data-root /data1/docker
docker info --format 'root={{.DockerRootDir}} architecture={{.Architecture}}'
docker compose version
systemctl is-enabled docker
systemctl is-active docker
```

安装脚本支持的系统包括 Ubuntu、Debian、CentOS、RHEL、Rocky Linux 和 AlmaLinux，实际发行版和软件源仍需在目标环境验证。该脚本会备份并覆盖已有 `daemon.json`、重启 Docker；已有工作负载的机器需先安排变更。`--reconfigure` 不迁移旧 data-root 中的数据或镜像，已有 Docker 的服务器应先处理迁移和其他配置合并，不能直接当作数据迁移工具。

各节点需具备 Bash、Docker Compose v2、`sha256sum`、util-linux 的 `findmnt` 和支持 `df -BG --output` 的 GNU 工具；节点脚本不需要 Python。确认 9092、9093、19092 未被其他服务占用。节点检查使用 `findmnt -T` 识别实际数据路径所在文件系统；目录尚未创建时检查最近的已有父目录，不会为了检查先创建目录。若结果落到系统根文件系统，生产检查会阻断。

## 3. 管理机生成生产部署包

**执行位置为管理机。** 管理机需要 Bash、Python 3、OpenSSL、base64、`sha256sum`、SSH 和 scp。本文以管理机仓库也位于 `/root/hr-deploy` 为例。

```bash
cd /root/hr-deploy/kafka
/root/hr-deploy/kafka/kafka-admin.sh cluster init
/root/hr-deploy/kafka/kafka-admin.sh cluster validate
/root/hr-deploy/kafka/kafka-admin.sh cluster info
```

配置保存至 `/root/hr-deploy/kafka/config/kafka-admin.conf`，节点包保存至 `/root/hr-deploy/kafka/cluster/nodes/`。

交互时按机器资源选择 `standard` 或 `minimal`，依次输入三个 controller 地址和三个 broker 地址。本文两组地址相同，顺序对应 node-1 至 node-3。确认镜像及 Kafka 数据基础目录，认证选 `true`，客户端端口和初始账号可沿用 `19092` 和 `admin`。首次部署密码留空会自动生成 6 位随机密码并保存到管理机配置及 `cluster/clients/admin.properties`，不会在终端显示。也可手动输入密码（不限制长度，不回显），使用 ASCII 字母、数字或 `_@%+=:./-`。已有配置留空沿用保存的密码；已有输出但配置缺失密码时需恢复原值，不自动生成替代密码。不要在 shell 命令参数中写真实密码。默认镜像为 `apache/kafka:4.3.2`，如改为内部镜像地址，所有节点必须使用生成包指定的同一镜像，部署前确认目标架构和镜像内 CLI 路径。

生成器只产生文件，不执行 SSH、拉取镜像或启动 Kafka。`cluster validate` 是静态校验，不能替代运行验收。保管管理机配置 `config/kafka-admin.conf` 和整个 `cluster/` 目录，两者均已在 Git 忽略规则中。三个 controller 必须来自同一批生成包。

重复生成会复用输出目录中的身份；改变集群名称本身不会创建新身份。现有数据必须继续使用原身份。修改初始密码后重新生成可能被拒绝；已有账号应按 [认证说明](authentication.md) 在线管理。

## 4. 分发到对应服务器

**执行位置为管理机。** 以下命令用于首次部署，将示例 IP 替换成初始化时填写的实际地址。

```bash
ssh root@10.0.0.11 'mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker'
scp -r /root/hr-deploy/kafka/cluster/nodes/controller-1-10.0.0.11/. root@10.0.0.11:/root/deploy/kafka/controller/
scp -r /root/hr-deploy/kafka/cluster/nodes/broker-1-10.0.0.11/. root@10.0.0.11:/root/deploy/kafka/broker/

ssh root@10.0.0.12 'mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker'
scp -r /root/hr-deploy/kafka/cluster/nodes/controller-2-10.0.0.12/. root@10.0.0.12:/root/deploy/kafka/controller/
scp -r /root/hr-deploy/kafka/cluster/nodes/broker-2-10.0.0.12/. root@10.0.0.12:/root/deploy/kafka/broker/

ssh root@10.0.0.13 'mkdir -p /root/deploy/kafka/controller /root/deploy/kafka/broker'
scp -r /root/hr-deploy/kafka/cluster/nodes/controller-3-10.0.0.13/. root@10.0.0.13:/root/deploy/kafka/controller/
scp -r /root/hr-deploy/kafka/cluster/nodes/broker-3-10.0.0.13/. root@10.0.0.13:/root/deploy/kafka/broker/
```

`/.` 包含隐藏的 `.env`。只复制对应节点包，不将管理机的 `admin.properties` 分发给所有服务器。

## 5. 拉取镜像、检查并格式化

**执行位置：每台目标服务器，root。** 三台服务器执行相同命令，各角色包内已记录自身的节点编号：

```bash
docker pull apache/kafka:4.3.2
docker image inspect apache/kafka:4.3.2 --format 'architecture={{.Architecture}} digests={{json .RepoDigests}}'
docker run --rm --network none --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.3.2 --version
bash /root/deploy/kafka/controller/deploy.sh check
bash /root/deploy/kafka/controller/deploy.sh prepare
bash /root/deploy/kafka/broker/deploy.sh check
bash /root/deploy/kafka/broker/deploy.sh prepare
```

每条命令成功后再执行下一条。如初始化时选择其他镜像，将上面的镜像地址全部替换为生成包记录的值。

离线环境提前导入与目标架构一致、名称与包配置一致的镜像，并跳过 `docker pull`。记录镜像摘要和架构，确认所有节点使用预期镜像。

`check` 会核对摘要、root 权限、Docker/Compose 可用性、Kafka 数据所在文件系统与使用率及已有身份，不检查固定总容量；生产数据盘使用率达到 85% 会阻断，达到 70% 会提示。它不自动配置防火墙，也不代替完整主机资源和兼容性验收。

`prepare` 首次执行会设置权限并格式化数据；controller 同时写入初始 SCRAM 凭据。发现已有且身份匹配的数据则跳过格式化；身份冲突或非空未格式化目录则停止。不要通过删除 `meta.properties` 或数据目录绕过这些检查。

## 6. 启动六个角色

**执行位置为管理机。** 三台服务器的六个角色都完成 `prepare` 后，逐条执行：

```bash
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh start'
ssh root@10.0.0.12 'bash /root/deploy/kafka/controller/deploy.sh start'
ssh root@10.0.0.13 'bash /root/deploy/kafka/controller/deploy.sh start'
ssh root@10.0.0.11 'bash /root/deploy/kafka/controller/deploy.sh health 120'
```

健康检查通过后，再执行：

```bash
ssh root@10.0.0.11 'bash /root/deploy/kafka/broker/deploy.sh start'
ssh root@10.0.0.12 'bash /root/deploy/kafka/broker/deploy.sh start'
ssh root@10.0.0.13 'bash /root/deploy/kafka/broker/deploy.sh start'
```

先发起全部三个 controller，随后运行 `health 120`，通过后才发起三个 broker。健康检查直接使用 controller 内部端口，不依赖 broker 或业务账号。不要在仅启动一个 controller 后就等待完整 quorum；容器启动命令返回不代表 Kafka 已就绪。

在任一服务器的 `/root/deploy/kafka/controller` 中执行 `/root/deploy/kafka/controller/deploy.sh health` 可单次检查；`/root/deploy/kafka/controller/deploy.sh health 120` 会在指定重试窗口内等待。检查要求三个入口都能查询，cluster ID 和 voter 清单符合本次部署，返回同一个合法 leader，三个 voter 的复制 lag 为 0 且角色正确。成功返回 0，查询失败、身份不符或复制未就绪返回非零。使用独立临时 CLI 容器，不占用 controller 容器的内存限额、不重启服务；单次请求超时可能使总耗时略超过重试窗口。

下一节仍需检查 broker 注册和后续读写。需要日志时执行 `ssh root@10.0.0.11 "bash /root/deploy/kafka/broker/deploy.sh logs"`，Ctrl-C 结束查看。

## 7. 生产基础状态检查

按 [通用消息收发验证](verify-messages.md) 第 1 节选择生产入口，在授权验证节点准备当前集群的客户端配置；第 2 节准备验证目录。随后执行第 3 节的 controller quorum、复制及 broker 注册检查。

不要运行本地 `tests/test-local.sh verify` 来替代生产验收：该脚本依赖测试拓扑和资源，还会更新测试账号、重启和重建 broker。

## 8. 专用 Topic 的读写验收

继续执行 [通用消息收发验证](verify-messages.md) 第 4–6 节。创建一个分区、三个副本、min ISR 2 的专用 Topic，等待 ISR 包含三个 broker，再以 `acks=all` 发送三条消息、消费并通过 `diff` 比对完整内容。生产和本地使用完全相同的 CLI 命令，不修改账号或重启服务。

记录 Topic、时间、集群身份、镜像摘要、quorum/ISR 输出及读写证据。验证默认保留 Topic，按需清理；该流程不会自动生成本地测试的 `result.json`。生产还需从实际应用网络检查三个 advertised 地址和应用账号的连接。

## 9. 故障恢复验收与后续运维

基础状态和读写验收不会触发故障。上线前还需在独立测试环境或安排好的维护窗口逐项验证：单 broker 重启/重建、单 controller 重启、单台服务器重启后的磁盘挂载及服务恢复、应用侧连接重试、实际业务容量。每次只操作一个节点，恢复 quorum、ISR 和读写后再进行下一项，并保留测试记录。

角色操作在对应服务器的包目录执行：

```bash
/root/deploy/kafka/broker/deploy.sh status
/root/deploy/kafka/broker/deploy.sh logs
/root/deploy/kafka/broker/deploy.sh restart
```

`restart` 仅重启已有容器，不重新应用 Compose 配置。配置更新需保留原身份、重新生成和分发完整包，并按变更流程逐节点执行 `stop` / `start` 重建容器；`stop` 不删除宿主数据。不要同时停多个 controller 或多个 broker。

持续监控磁盘用量、离线分区、ISR 收缩、controller quorum、进程重启和 OOM。保存原始配置、manifest、节点包和凭据；部署包备份不等于 Kafka 数据备份。故障时优先查看对应角色日志，身份冲突时恢复匹配的部署信息，不清空数据来重新初始化。
