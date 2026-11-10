# Kafka 三节点集群部署方案

依据 [spec.md](spec.md)，本轮交付部署方案及 pre 阶段确认项；脚本和 Compose 文件待方案确定后实现。

## 1. 版本与部署拓扑

默认使用官方 JVM 镜像 `apache/kafka:4.3.2`，固定版本，不跟随 `latest`。截至 2026-11-10，官方下载页列出了 4.4.0 和 4.3.2，4.3.2 是受支持的 4.3 系列补丁版本。pre 允许修改镜像仓库和版本；生成时校验镜像可拉取、目标 CPU 架构及初始化参数兼容性。未来实现先保证 4.3.2 的验收，其他版本需通过兼容性检查，不能仅替换 tag 就宣称支持。[官方下载](https://kafka.apache.org/community/downloads/)、[官方 Docker 文档](https://kafka.apache.org/43/getting-started/docker/)

将“三节点”解释为 **3 个 broker、3 个 controller，默认放在 3 台 VM**，共 6 个独立 Kafka 进程。采用 KRaft；每个进程只承担一个角色。controller 和 broker 使用各自的 Compose 项目、配置目录、数据目录和操作脚本。

| VM（每台 8 vCPU / 32 GiB） | controller | broker |
| --- | --- | --- |
| vm-1 | controller-1，node.id=101 | broker-1，node.id=1 |
| vm-2 | controller-2，node.id=102 | broker-2，node.id=2 |
| vm-3 | controller-3，node.id=103 | broker-3，node.id=3 |

上述地址是占位名称，pre 必须填写真实地址。6 个 `node.id` 在集群内保持唯一。同一份方案可将 controller 部署包移动到另外 3 台 VM，但必须在 pre 中登记实际地址并重新生成一致配置；不会根据 scp 目标自动推断 Kafka 地址。

3 个 controller 需要多数派，能够容忍 1 个 controller 故障。默认共置方式中，一台 VM 故障会同时失去一个 controller 和一个 broker；控制面仍有多数派，数据面能否持续写入还取决于副本、ISR 和客户端配置。若 3 台 VM 共用同一物理宿主机，仍存在共同故障点。[KRaft 角色及多数派说明](https://kafka.apache.org/43/operations/kraft/)

## 2. 初始资源预算

以下为方案建议值，pre 可调整，实际吞吐能力需压测确定。以每台 VM 同时运行一个 controller 和一个 broker 为预算单位：

| 用途 | CPU 上限/预算 | 内存上限/预算 | JVM 堆 |
| --- | --- | --- | --- |
| controller | 1.5 vCPU | 6 GiB | Xms=Xmx=2 GiB |
| broker | 5.5 vCPU | 22 GiB | Xms=Xmx=6 GiB |
| OS、Docker、监控及余量 | 至少 1 vCPU | 至少 4 GiB | — |

Compose 使用服务级 `cpus`、`mem_limit` 实施容器限制；两容器上限合计为 7 vCPU / 28 GiB。CPU 限额不是独占核，OS 预留是预算余量。部署前检查宿主机其他服务占用，避免超卖。

broker 的 22 GiB 包含堆、堆外、线程及计入容器的文件页缓存，不能全部分配为 JVM 堆。controller 上限也需保留堆外余量。拆分到独立 VM 后默认沿用这一预算，不自动放大 JVM。

磁盘方面，controller 建议单独的持久化元数据目录，初始规划至少 20 GiB；broker 使用 SSD 数据盘，容量必须在 pre 中根据业务确认。建议按下式估算，额外核算分区索引、内部主题和数据倾斜：

`单 broker 数据盘容量 >= 集群压缩后写入字节/秒 × 保留秒数 × 副本数 / broker 数 / 0.70`

这里预留约 30% 磁盘余量；保留策略不是硬容量保护。数据目录不能落到容器可写层。controller 元数据与 broker 数据使用不同目录，条件允许时分盘，避免数据盘繁忙拖慢控制面。文件描述符初始建议 `nofile=100000`，检查时间同步、磁盘挂载和 `vm.max_map_count` 是否满足分区规模。[官方硬件与 OS 说明](https://kafka.apache.org/43/operations/hardware-and-os/)

## 3. 网络与监听器

目标环境采用 Linux VM + Docker Engine + Compose v2，默认 `network_mode: host`。每个角色是本机 Compose 项目；跨 VM 通信使用真实可路由地址，不依赖 Compose 服务名或默认 bridge 网络。macOS 上可生成部署包，目标运行环境按 Linux 验证。

| 监听器 | 默认端口 | 用途与访问范围 |
| --- | --- | --- |
| CONTROLLER | 9093 | controller 间及 broker 到 controller 的通信，限制在集群网络 |
| INTERNAL | 9092 | broker 间通信、controller 到 broker 的通信及内网客户端访问 |
| EXTERNAL（可选） | 19092 | 客户端处于另一网络时启用，填写每个 broker 的独立可达地址 |

端口可在 pre 修改。controller 只监听 CONTROLLER；broker 监听 INTERNAL 及可选 EXTERNAL，通过 `controller.listener.names` 连接 controller，不能在 broker 上额外绑定 CONTROLLER 端口。两类角色均配置所需的 `listener.security.protocol.map`；controller 还需具备访问 broker INTERNAL 监听器的客户端安全配置。

`listeners` 定义本机绑定地址；broker 的 `advertised.listeners` 必须使用客户端及其他 broker 真正能访问的地址，不允许填写 `0.0.0.0`、仅本机可达的 localhost 或跨机器不可解析的容器名。bootstrap 地址只能帮助客户端发现集群，客户端最终必须能够连接每个 broker 的 advertised 地址。[监听器说明](https://kafka.apache.org/43/security/listener-configuration/)

## 4. pre 阶段交互确认项

交互按“必填信息 → 带默认值的业务参数 → 高级参数 → 汇总确认”进行。无真实地址、磁盘容量和安全模式时，只能生成草案，不能生成标记为可部署的包。

### 4.1 部署环境

| 确认项 | 建议或行为 |
| --- | --- |
| 集群名称、输出目录 | 必填；决定 Compose 项目命名和生成目录 |
| 镜像仓库、tag、架构 | 默认 `apache/kafka:4.3.2`；可指定私有镜像仓库，支持记录拉取后的 digest |
| 角色与 VM 对应关系 | 默认 3 台共置；分别填写 3 个 controller 和 3 个 broker 的部署目标 |
| SSH/SCP 信息 | 手动分发时可省略；自动分发填写主机、端口、用户、密钥引用、远端目录 |
| 地址和端口 | 分别确认 SSH 地址、集群内地址、客户端 advertised 地址、DNS 和防火墙连通性 |
| 宿主机资源 | CPU、内存、已有进程预算、数据盘大小和挂载点；按宿主机汇总校验 |
| 宿主机目录及 UID/GID | 部署目录、broker 数据目录、controller 元数据目录、证书目录及容器用户写权限 |
| 资源限制与堆 | 默认采用第 2 节预算；校验堆小于容器内存上限 |
| 分发方式 | 默认生成包后人工复制；显式选择 scp 时只复制文件，不启动服务 |
| 日志和监控 | 应用日志默认输出 stdout/stderr，Docker 日志轮转建议 50 MiB × 5；确认是否接入现有监控 |

### 4.2 Kafka 服务关键配置

下列建议是本方案起点；配置项含义以 [Kafka 4.3 broker 配置参考](https://kafka.apache.org/43/configuration/broker-configs/) 为准。pre 汇总中要显示最终值及其作用域。

| 类别 | 配置或信息 | 建议及需要确认的内容 |
| --- | --- | --- |
| 身份 | `process.roles`、`node.id`、cluster ID | 按拓扑自动生成并展示；cluster ID 全集群唯一且一致，生成后锁定 |
| 控制面 | `controller.quorum.bootstrap.servers`、`controller.listener.names` | 登记全部 3 个 controller 的地址；使用动态 quorum |
| 首次初始化 | 3 个 controller directory ID、initial controllers 清单 | pre 生成一次并持久化，6 个部署包使用同一份集群身份清单 |
| 通信 | `listeners`、`advertised.listeners`、`listener.security.protocol.map`、`inter.broker.listener.name` | 按第 3 节确认内外网络与协议；inter-broker 使用 INTERNAL |
| 持久化 | `log.dirs`、`metadata.log.dir` | 渲染为容器内绝对路径，映射到已确认的宿主机持久目录；broker 元数据也必须持久化 |
| 默认副本 | `default.replication.factor=3`、`min.insync.replicas=2` | 默认按容忍一台 broker 故障设计，业务 producer 配合 `acks=all` |
| 内部主题 | `offsets.topic.replication.factor=3`、`transaction.state.log.replication.factor=3`、`transaction.state.log.min.isr=2` | 首次创建前确定；如使用 Share Groups，另确认对应 state topic 的副本和 min ISR |
| 数据安全 | `unclean.leader.election.enable=false` | 初始建议关闭不洁选主；保留目标版本 ELR 特性基线并在验收时记录 |
| 默认分区 | `num.partitions=3` | 仅作为未指定分区数时的起点；另确认预期 topic 数、总分区数及消费并行度 |
| 自动建主题 | `auto.create.topics.enable=false` | 建议显式创建业务主题，避免误拼名称产生主题 |
| 保留策略 | `log.retention.hours=168`、`log.retention.bytes=-1`、`log.cleanup.policy=delete` | 7 天只是建议，需结合真实容量确认；bytes 是每分区限制，不是整块盘限制 |
| 日志分段 | `log.segment.bytes=1073741824`、`log.roll.hours=168` | 建议 1 GiB / 7 天；结合低流量主题和清理时效确认，删除并非精确在保留时间点发生 |
| 消息大小 | `message.max.bytes`、`replica.fetch.max.bytes` | 初始沿用 4.3.2 默认值，汇总显示数值；大消息需求需同步核对 producer、consumer 和 topic 配置 |
| 压缩 | `compression.type=producer` | 建议由 producer 决定；确认业务是否采用 lz4/zstd 等压缩 |
| 线程与复制 | `num.network.threads`、`num.io.threads`、`num.replica.fetchers` | 初始沿用目标版本默认值，按吞吐、分区数和磁盘性能压测后再调整 |
| 故障域 | `broker.rack` | 有机架/可用区信息时必填真实值，不能用虚构标签宣称跨域容灾 |

副本数 3、min ISR 2、`acks=all` 的组合要求 producer 配合；部署脚本不能替业务客户端设置 acks。已有 topic 的显式配置可能覆盖 broker 默认值，更改默认副本数也不会自动重分配已有 topic。[副本与写入确认语义](https://kafka.apache.org/43/configuration/broker-configs/#min.insync.replicas)

### 4.3 安全配置

安全模式作为 pre 必选项，不能静默替用户决定。生产建议 SSL/mTLS 与 ACL；隔离测试网络可以显式选择 PLAINTEXT。分别确认客户端、inter-broker 和 controller 通道的协议及身份。

若启用 TLS，收集 CA、节点证书及 SAN、keystore/truststore 引用与密码文件；若选择 SASL_SSL，另收集机制、服务身份和凭据初始化方式。启用授权时确认 `StandardAuthorizer`、管理员及集群内部主体的 `super.users` 和初始 ACL，保证首次启动的控制面与 broker 能互通。私钥和密码文件独立挂载、限制文件权限，汇总输出脱敏；`.env` 保存引用路径。

## 5. 配置、部署包与脚本设计

以下为后续实现的目标目录，不代表本轮已生成：

```text
kafka/
  docs/
    spec.md                      # 部署需求
    plan.md                      # 部署方案
  pre.sh                         # 交互收集、校验并生成包
  deploy.sh                      # 可选 scp 分发，不启动
  templates/                     # Compose、properties、角色脚本模板
  generated/<cluster>/
    .env                         # 全局部署参数和 pre 输入记录
    manifest.json                # 身份、节点映射、版本、配置摘要
    README.md                    # 本次集群的操作顺序和地址清单
    controller-1/                # controller-2、controller-3 同构
      .env
      compose.yaml
      config/controller.properties
      config/log4j2.yaml
      bin/{prepare,start,stop,status,logs}.sh
    broker-1/                    # broker-2、broker-3 同构
      .env
      compose.yaml
      config/broker.properties
      config/log4j2.yaml
      bin/{prepare,start,stop,status,logs}.sh
```

每个角色目录可独立分发和执行，无需读取兄弟目录；使用不同 Compose 项目名，避免同机部署互相覆盖。每份 `.env` 包含该角色运行需要的镜像、项目名、资源、堆、宿主机挂载路径以及持久化的集群身份引用；Kafka 配置从 pre 输入渲染为宿主机可见的 properties 文件。

Compose 的 `.env` 负责变量插值，不会自动替换外部 properties 中的占位符。因此 Kafka 挂载配置必须是完整的最终值，不留 `${...}`。生成后修改 Kafka 参数需重新渲染、展示差异并校验；启动时检查配置摘要，避免误以为只改 `.env` 就能改变已渲染的 properties。直接编辑挂载配置也需显式更新摘要并通过校验。[Compose 环境变量说明](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)

计划将配置只读挂载到自定义路径，例如 `/etc/kafka/custom/`；显式指定启动入口，调用镜像内 `kafka-server-start.sh` 并传入该 properties。JVM 环境变量由 Compose 显式传入。这样可以明确控制格式化与启动，避免官方默认入口的配置生成行为覆盖挂载文件。实现时需核对镜像内实际路径、权限及启动行为，并验证 PID 1 的信号转发和优雅退出。[官方镜像入口源码](https://raw.githubusercontent.com/apache/kafka/4.3.2/docker/resources/common-scripts/run)

生成目录默认排除出版本控制，`.env` 权限建议 0600；manifest 记录摘要和非敏感身份信息，不能记录密码或私钥。

## 6. 阶段流程与重复执行规则

### pre：生成部署计划和节点包

1. 交互收集第 4 节信息，检查重复 ID、地址/端口冲突、每宿主机资源总量、路径冲突、证书完整性和镜像兼容性。
2. 新集群生成一次 cluster ID 和 3 个 controller directory ID，固定初始 controller 清单；重新执行 pre 时复用已有身份，不重建集群 ID。
3. 展示拓扑、资源、安全模式、存储容量估算及最终配置，用户确认后输出 6 个独立包、部署说明和 manifest。
4. 本地可验证模板及 Compose；只有在目标节点才能完成的磁盘、权限、端口和跨主机连接检查，交给 prepare/启动检查执行，不能提前声称已通过。

### deploy：分发

支持人工复制各角色目录，也支持按清单用 scp 分发。分发到新的版本目录并校验摘要，不覆盖运行中配置、数据和证书；失败逐节点报告，允许只重试失败目标。SSH 保持主机密钥校验。此阶段不启动、不格式化、不清理数据。

### 指定节点 prepare 与 start：首次启动

1. 在目标 VM 检查 Docker/Compose、架构、资源、挂载点、目录权限及绑定端口；确认数据盘真实挂载，防止数据意外写入根盘。镜像拉取或按需离线加载；准备数据目录。
2. 首次格式化单独由角色 `prepare.sh` 执行。新集群的 3 个 controller 均使用相同 cluster ID 和相同 `--initial-controllers` 清单；broker 使用相同 cluster ID 和 `--no-initial-controllers`。动态 quorum 配置使用 `controller.quorum.bootstrap.servers`，不设置 `controller.quorum.voters`。[官方格式化流程](https://kafka.apache.org/43/operations/kraft/#provisioning-nodes)
3. 已有 `meta.properties` 时检查 cluster ID、node ID 及相关目录身份，一致则跳过格式化；发现身份不一致、非空但未完成格式化或部分目录初始化失败时停止并报告。正常重启不生成身份，不使用无条件 `--ignore-formatted`，不自动删除数据。
4. 分别在 3 个 controller 节点执行 start。首个 controller 启动时可能等待多数派，不应阻塞其他 controller 的启动；启动至少两个后才检查选主，最终要求全部 3 个加入并追平。
5. 控制面稳定后分别启动 3 个 broker，确认全部注册，再执行集群验收。跨 VM 没有 Compose `depends_on` 协调，由操作说明明确顺序；角色脚本只负责本节点启动和本地状态。

这里选择一次初始化 3 个 controller，适合本次固定三节点部署；官方也支持先建单 controller 再动态添加。将来替换或增加 controller 需使用加入/移除成员流程，不能把新空盘节点当成最初 voter 重新引导。

普通 start 仅启动已 prepare 且身份匹配的目录；stop 优雅停止并保留数据，默认建议预留 120 秒退出时间。配置变更采用逐节点重启，每次等待恢复健康；镜像升级需独立核对升级文档，不能通过改 tag 隐式升级元数据特性。

## 7. 后续实现验收标准

- 生成 6 份可独立运行的角色包；默认共置和角色分置两种拓扑的地址、项目名、端口、挂载均正确。
- `docker compose config --quiet`、Shell 语法检查及生成配置一致性检查通过；错误输入、身份冲突和重复生成有明确结果。
- 目标 Linux 环境内使用真实 4.3.2 镜像确认挂载、资源限制、信号退出和首次格式化行为；本机只有模板校验时，明确记录未完成运行验收。
- quorum 有 leader、3 个 voters、健康的复制状态；3 个 broker 可通过 API 发现。角色健康检查使用 Kafka API/CLI，不只检查容器 running 或 TCP 端口。
- 经客户端实际网络创建验收专用主题（3 分区、3 副本、min ISR 2），使用 `acks=all` 生产并消费验证。若启用 TLS/ACL，使用与业务一致的认证路径验证。
- 停止一台 VM 上的 controller 和 broker 后验证多数派及持续读写；恢复后等待 ISR 和 quorum 追平。测试停止两个 broker 时写入按预期受限，并在完成后恢复全部节点。
- 验证容器重建和宿主机重启后 cluster ID、元数据及消息仍保留；重复 prepare 不重格式化。
- 验证配置包重复分发、部分分发失败及摘要不匹配时不会覆盖当前运行数据。

本轮产物仅为本方案；真实节点地址、安全模式、数据盘容量、业务保留时间及吞吐/分区目标，将作为后续 pre 交互输入。
