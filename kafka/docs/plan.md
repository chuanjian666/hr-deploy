# Kafka 集群部署方案

本文依据 [spec.md](spec.md) 说明 Kafka 集群的部署结构、配置项、操作流程和验收标准。管理机初始化和节点部署脚本已经按本方案实现；已完成静态校验及 `test` 档在本地 3 个隔离 Linux 节点中的实际部署和功能验证，详见 [验证记录](local-test.md#验证记录)。尚未完成三台独立 VM 或生产验收。

## 1. 设计结论

| 项目 | 方案 |
| --- | --- |
| 部署目标 | 3 个 broker + 3 个 controller，共 6 个独立 Kafka 进程 |
| 默认拓扑 | 3 台 Linux VM，每台共置 1 个 controller 和 1 个 broker；生产规格 8C32G / 4C16G，测试规格 2C4G |
| 角色模型 | KRaft，controller 与 broker 分离，不使用 combined role |
| 部署方式 | Docker Compose v2，默认使用 host 网络 |
| 默认镜像 | `apache/kafka:4.3.2`，允许在集群初始化（`cluster init`）阶段替换完整镜像地址 |
| 管理入口 | `kafka-admin.sh`，负责配置、节点包生成、信息展示和集群验证 |
| 节点入口 | 每个角色包内独立的 `deploy.sh`，负责该角色的检查、初始化和生命周期管理 |
| 生成目录 | `kafka/cluster/nodes/<role>-<序号>-<host>/` |
| 分发方式 | 默认由管理员人工复制节点包；本轮不实现自动 SSH/scp 分发 |
| 配置方式 | 管理机配置、节点 `.env`、Kafka properties 三层分离 |
| 安全模式 | 初始化可选客户端 SCRAM 认证；内部通信保持 `PLAINTEXT`，不启用 TLS 或 ACL；仅部署在可信隔离内网 |

默认拓扑如下：

| VM | controller | broker |
| --- | --- | --- |
| node-1 | controller-1，`node.id=101` | broker-1，`node.id=1` |
| node-2 | controller-2，`node.id=102` | broker-2，`node.id=2` |
| node-3 | controller-3，`node.id=103` | broker-3，`node.id=3` |

controller 和 broker 的目标 VM 分别配置，因此也可以将 3 个 controller 与 3 个 broker 部署到 6 台 VM。节点包不根据 scp 目标推断地址；角色地址发生变化时必须重新执行 `cluster init` 并重新生成一致配置。

## 2. 设计策略

Kafka 部署工具遵循以下设计策略：

1. **统一入口**：使用 `kafka-admin.sh` 作为管理机入口，集中提供集群初始化、配置查看、部署包分发和集群验证命令。
2. **职责分层**：`config/` 保存管理机配置，`templates/` 保存模板，`scripts/lib/` 提供公共能力，`scripts/core/` 编排集群操作。CLI 只负责解析命令。
3. **生成与运行分离**：`cluster init` 只在管理机采集和校验配置、生成节点包，不启动本地或远端 Kafka 服务。
4. **节点包独立**：所有生成包放在 `cluster/nodes/`。每个角色包包含运行所需的完整文件，可以独立复制、检查和启动，不依赖其他角色包的本地文件。
5. **角色独立运维**：每个节点包使用顶层 `deploy.sh`，提供 `check`、`prepare`、`start`、`stop`、`restart`、`status` 和 `logs`。
6. **环境准备解耦**：Docker 安装由仓库根目录的 `docker/setup-docker.sh` 独立完成，不嵌入 Kafka 部署工具。节点脚本只检查 Docker Engine、daemon 访问权限和 Compose v2 是否可用。
7. **配置显式化**：部署参数保存在 `.env`，Kafka 服务配置渲染为完整 properties 文件。模板变量、默认值和最终运行值均可检查。
8. **身份稳定**：cluster ID、node ID 和 controller directory ID 一经生成便持久保存。重复生成节点包不能无条件创建新身份。
9. **操作安全**：格式化与启动分离；重新生成时不无条件删除 `cluster/`；脚本不自动清理目标机数据。
10. **状态可验证**：不能仅以容器运行或端口可连接判定集群健康，还要检查 controller quorum、broker 注册及实际生产消费。
11. **产物受控**：生成目录包含部署环境和集群身份信息，必须被 Git 忽略并限制文件权限。

Kafka 角色层面的约束如下：

- 普通 `deploy` 不隐式格式化数据目录；首次格式化由独立的 `prepare` 完成，并检查已有身份。
- controller 与 broker 不合并为一个 Compose 服务。两种角色使用独立项目名、目录和节点包。
- controller 或 broker 即使共置在同一台 VM，也可以分别分发、启动、停止和恢复。
- 跨主机依赖通过部署顺序和健康验证管理，不使用 Compose `depends_on` 表达。

## 3. 分层架构与目录

当前仓库结构：

```text
├── docker/                            # Kafka 等服务共用的 Docker 基础环境
│   ├── README.md
│   └── setup-docker.sh
└── kafka/
    ├── kafka-admin.sh                    # 管理机统一 CLI
    ├── README.md                         # 使用、部署和运维说明
    ├── config/
    │   ├── kafka-admin.conf.example      # 可提交的默认配置
    │   └── kafka-admin.conf              # 本地实际配置，Git 忽略、权限 0600
    ├── docs/
    │   ├── spec.md
    │   └── plan.md
    ├── scripts/
    │   ├── lib/
    │   │   ├── common.sh                 # 日志、文件、命令检查
    │   │   ├── config.sh                 # 安全加载及保存配置
    │   │   └── validation.sh             # 地址、端口、资源、拓扑和 Kafka 参数校验
    │   └── core/
    │       └── cluster-engine.sh          # 集群初始化、节点包生成、集群信息和静态验证
    ├── templates/
    │   ├── controller.properties.tmpl
    │   ├── broker.properties.tmpl
    │   ├── log4j2.yaml.tmpl
    │   ├── docker-compose.yml.tmpl
    │   └── deploy.sh.tmpl
    └── cluster/                           # 生成内容，整体 Git 忽略
        ├── manifest.json                   # 集群身份和生成版本；权限 0600
        ├── topology.csv                   # 角色、node ID、地址与包目录映射
        ├── README.md                      # 本次部署的准确操作顺序
        └── nodes/
            ├── controller-1-<host>/
            ├── controller-2-<host>/
            ├── controller-3-<host>/
            ├── broker-1-<host>/
            ├── broker-2-<host>/
            └── broker-3-<host>/
```

每个角色包结构一致：

```text
<role>-<n>-<host>/
├── deploy.sh
├── docker-compose.yml
├── .env
├── manifest.json
├── checksums.sha256
└── config/
    ├── server.properties
    └── log4j2.yaml
```

角色包不得依赖兄弟目录。controller 与 broker 即使位于同一 VM，也复制到 `/root/deploy/kafka/` 下的不同角色目录，并使用不同 Compose project name、容器名和持久化数据目录。

## 4. 命令和阶段模型

### 4.1 管理机 CLI

```bash
./kafka-admin.sh cluster init          # 集群初始化：交互确认并生成节点包
./kafka-admin.sh cluster info          # 显示拓扑、身份摘要和配置摘要
./kafka-admin.sh cluster validate      # 静态校验生成包，不连接 Kafka

./kafka-admin.sh config show           # 敏感值脱敏显示
./kafka-admin.sh config validate       # 只校验，不生成
./kafka-admin.sh config set KEY VALUE
./kafka-admin.sh config reset
```

文档、命令说明和日志统一使用“集群初始化（`cluster init`）阶段”。

### 4.2 节点角色脚本

```bash
./deploy.sh check                      # 只做目标机前置检查
./deploy.sh prepare                    # 首次格式化；可安全重复执行
./deploy.sh start
./deploy.sh stop
./deploy.sh restart
./deploy.sh status
./deploy.sh logs
```

不带参数时只显示帮助，不自动执行 `prepare` 或 `start`。这一点避免误复制节点包后直接改写已有数据目录。

### 4.3 完整流程

```text
集群初始化（`cluster init`）阶段
  -> 配置校验与汇总确认
  -> 生成 6 个角色包
  -> 人工复制节点包
  -> 各目标 VM 执行 deploy.sh check
  -> 3 个 controller 执行 deploy.sh prepare
  -> 3 个 broker 执行 deploy.sh prepare
  -> 启动至少 2 个 controller，再启动第 3 个
  -> 确认 controller quorum
  -> 依次启动 3 个 broker
  -> 部署完成后按第 14.2 节人工验收
```

跨 VM 启动顺序由本次生成的 `cluster/README.md` 明确给出，不使用 Compose `depends_on` 模拟跨主机依赖。

## 5. 配置分层

### 5.1 管理机配置

`config/kafka-admin.conf` 保存集群初始化输入和默认值，包括拓扑、镜像、资源、端口、宿主机目录和业务默认值。它不直接作为容器 `env_file`。

该文件是受信任的 Bash 配置，只允许加载本工具生成或管理员审核过的文件。实际配置可能含 SSH 用户等环境信息，权限设置为 `0600`，不提交 Git。

### 5.2 节点 `.env`

每个节点包的 `.env` 只保存 Compose 插值和节点运行参数，例如：

- 镜像、Compose project name、容器名；
- CPU、内存、JVM heap、日志轮转；
- 宿主机数据和配置目录；
- 节点角色、node ID、cluster ID。

`.env` 不承担 Kafka properties 模板替换。Compose 不会自动替换外部 properties 文件中的 `${...}`。

### 5.3 Kafka 服务配置

`cluster init` 直接渲染完整的 `config/server.properties`，其中不保留模板变量。每个节点包的 manifest 记录配置文件 SHA-256。`start` 前检查摘要；管理员修改服务配置后必须显式更新摘要或重新生成包，避免 `.env` 与实际配置不一致。

输出摘要、日志和 `config show` 不泄露 SSH 等部署环境信息。

## 6. KRaft 身份与初始化

所有进程属于同一个 KRaft 集群：

- 全集群只有一个 `cluster.id`。
- 6 个 `node.id` 全局唯一。
- 每个 controller 有独立且持久化的 directory ID。
- 3 个 controller 使用相同的 initial controller 清单。
- broker 使用相同 cluster ID，但不作为 initial controller。

首次生成时创建并保存这些身份。再次运行 `cluster init` 时：

1. 若 `cluster/manifest.json` 已存在，默认复用 cluster ID、node ID 和 controller directory ID。
2. 拓扑变更先展示身份和地址差异，再要求确认。
3. 不提供普通参数重新生成 cluster ID。
4. 如需创建全新集群，使用新的集群名称或新的输出目录。
5. 不自动删除或清空任何目标机数据目录。

`deploy.sh prepare` 执行规则：

- controller 使用相同 cluster ID 和 initial controller 清单格式化本地元数据目录；
- broker 使用相同 cluster ID，并按目标 Kafka 版本支持的非 controller 初始化方式格式化；
- 已存在 `meta.properties` 时核对 cluster ID、node ID 和 directory ID，一致则跳过；
- 身份不一致、目录非空但未正确格式化、多个数据目录状态不一致时立即停止；
- 普通 `start` 只允许启动已成功 prepare 的目录。

目标版本具体使用的 `kafka-storage.sh format` 参数必须在实现时通过 `apache/kafka:4.3.2` 实际镜像验证，不能只依据其他 Kafka 版本推断。controller 替换或扩容属于单独运维流程，不得重新执行初始引导清单来冒充成员变更。

## 7. 网络与监听器

目标运行环境为 Linux VM、Docker Engine 和 Compose v2。默认使用 `network_mode: host`，跨 VM 通信全部使用真实可路由地址。目标机未安装 Docker 时，先使用仓库根目录 `docker/setup-docker.sh` 完成基础环境准备。

| 监听器 | 默认端口 | 作用 |
| --- | ---: | --- |
| `CONTROLLER` | 9093 | controller quorum，以及 broker/controller 控制面通信 |
| `INTERNAL` | 9092 | broker 间复制；关闭认证时也可供内网客户端访问 |
| `EXTERNAL` | 19092 | 客户端入口；开启认证时自动启用，所有业务客户端均使用此入口 |

监听器规则：

- controller 只监听 `CONTROLLER`。
- broker 监听 `INTERNAL`；开启认证时自动增加 `EXTERNAL`，关闭认证时由 `ENABLE_EXTERNAL_LISTENER` 决定；不监听 `CONTROLLER`。
- `listeners` 可以绑定 `0.0.0.0`；`advertised.listeners` 不允许填写 `0.0.0.0`、localhost 或客户端无法解析的容器名。
- `cluster init` 分别采集 SSH 地址、集群内通信地址和客户端 advertised 地址，三者不能混为一个字段。
- 启用 `EXTERNAL` 时，每个 broker 必须有独立且从客户端网络可达的地址和端口，不能用一个普通四层地址替代所有 broker advertised 地址。
- `cluster init` 检查同一宿主机端口冲突；目标机 `check` 检查实际占用；跨主机防火墙和路由在启动前人工确认。

默认端口放行矩阵：

| 来源 | 目标 | 端口 |
| --- | --- | ---: |
| 3 个 controller、3 个 broker | 3 个 controller | 9093 |
| 3 个 broker、controller；关闭认证时可包含授权内网客户端 | 3 个 broker | 9092 |
| 指定业务客户端和运维网段 | 3 个 broker | 19092（开启认证时必须使用） |
| 监控系统 | JMX exporter | `cluster init` 指定端口（启用时） |

所有端口只对必要源网段开放。工具不自动修改云安全组、主机防火墙、DNS 或路由。

## 8. 资源与存储规划

集群初始化时必须选择部署规格，默认选择标准规格。这里的“最小生产规格”是本方案针对三节点、controller/broker 共置、低吞吐场景定义的受限基线，不是 Kafka 官方给出的通用最低配置。Kafka 没有脱离吞吐、分区数、消息大小、保留时间和磁盘性能的统一生产最小值。

### 8.1 规格档位

| 规格 | 单台 VM | 每台数据盘 | 适用范围 | 使用要求 |
| --- | --- | --- | --- | --- |
| 标准 `standard` | 8 vCPU / 32 GiB | 512 GB HDD | 默认生产规格 | 适用于 HR 内部消息队列的常规负载，仍需观察实际吞吐、分区和磁盘时延 |
| 最小 `minimal` | 4 vCPU / 16 GiB | 512 GB HDD | 低吞吐、较少 topic 和分区的小型生产集群 | 严格控制同机其他服务，并在上线前完成容量压测和单节点故障测试 |
| 测试 `test` | 2 vCPU / 4 GiB | 至少 20 GB，可与系统盘共用 | 仅本地 3 节点部署脚本及 Kafka 功能验证 | 每台共置 1 controller + 1 broker；仅少量消息，不用于生产，不做性能测试 |

本方案提供以上三个固定档位，不在集群初始化阶段提供 `custom` 规格。默认仍为 `standard`，测试档必须显式选择 `test` 并设置 `EXPECTED_DATA_DISK_GB=20`。实际资源不符合任一档位时，应先重新评估并修改方案，而不是绕过校验。

低于单台 4C16G 的 controller/broker 共置节点不定义为生产规格。`test` 是无吞吐要求的功能验证预算，已在 3 个隔离 Linux 节点中验证基础功能，不能据此推断任何生产容量或独立 VM 的故障隔离行为。

### 8.2 角色资源分配

标准规格：

| 用途 | CPU limit | memory limit | JVM heap |
| --- | ---: | ---: | ---: |
| controller | 1 vCPU | 2 GiB | 1 GiB |
| broker | 6 vCPU | 24 GiB | 8 GiB |
| OS、Docker、监控和余量 | 至少 1 vCPU | 至少 6 GiB | — |

最小生产规格：

| 用途 | CPU limit | memory limit | JVM heap |
| --- | ---: | ---: | ---: |
| controller | 1 vCPU | 2 GiB | 1 GiB |
| broker | 2 vCPU | 10 GiB | 4 GiB |
| OS、Docker、监控和余量 | 至少 1 vCPU | 至少 4 GiB | — |

这些数值是容器上限和初始预算，不是吞吐保证。broker 依赖文件页缓存，不能把容器内存全部分给 JVM。最小规格中 broker 保留约 6 GiB 给堆外内存、线程和页缓存；如果同机还运行采集器、数据库或其他数据服务，则应使用标准规格。

测试规格（每台 VM）：

| 用途 | CPU limit | memory limit | JVM heap |
| --- | ---: | ---: | ---: |
| controller | 0.5 vCPU | 512 MiB | 256 MiB |
| broker | 1 vCPU | 1536 MiB | 512 MiB |
| OS、Docker 和临时 CLI 余量 | 0.5 vCPU | 2 GiB | — |

3 台测试 VM 合计 6 vCPU / 12 GiB 内存 / 至少 60 GB 磁盘，宿主机另外保留自身资源。临时 Kafka CLI 使用 128–256 MiB heap，避免与 broker 争抢内存。保留 controller/broker 分离、3 副本和 min ISR 2，不以合并角色或降低副本数节省资源。

角色拆分到独立 VM 时，不能机械沿用共置节点的总规格。应按角色工作量重新评估；controller 的资源主要受 broker 数量、topic/分区规模及元数据变更频率影响，broker 的资源主要受吞吐、连接数、分区副本和磁盘性能影响。

### 8.3 运行和校验规则

Compose 使用 `cpus`、`mem_limit` 和 `ulimits.nofile`。初始 `nofile` 建议 100000；目标机 `check` 同时检查时间同步、磁盘挂载、剩余空间、文件系统权限和 Docker data-root 空间。

选择最小生产规格时，上线前需额外确认：预期峰值吞吐、topic 数、总分区数、平均消息大小、客户端连接数、保留时间和数据盘性能。工具不能在没有基准测试的情况下判定容量一定充足。`test` 不做这些容量和性能验收，只执行基础功能验证。

`test` 的节点检查允许 `/data1` 是普通目录，Docker data-root 仍为 `/data1/docker`；所在文件系统需至少 20 GB 容量（允许 1 GB 容量取整误差）和 5 GB 可用空间。目录、身份和文件摘要校验继续执行。`standard` / `minimal` 继续要求 `/data1` 独立挂载、Docker data-root 为 `/data1/docker`、至少 512 GB 数据盘。

最小规格上线前至少验证：

- 正常峰值负载下 CPU、GC、磁盘时延和 request queue 无持续饱和；
- 停止一台 VM 后仍满足业务读写目标；
- 故障节点恢复时，副本追赶不会使存活 broker 长时间过载；
- 7 天或实际保留周期的磁盘增长符合容量估算；
- 总分区数和连接数增长到规划上限时仍保留资源余量。

### 8.4 存储规划

两个生产规格均默认每台 VM 只配置一块 512 GB HDD，挂载到 `/data1`。Docker data-root 和 Kafka 持久化数据使用这块盘，通过子目录隔离；Kafka 部署包、Compose 和配置放在系统盘 `/root/deploy/kafka`。HDD 适用于当前 HR 内部系统消息队列的低吞吐场景，不建议再在该盘上运行数据库等高 I/O 服务。`test` 可直接使用至少 20 GB 的 VM 系统盘，手动创建 `/data1` 即可，保留相同 Kafka 目录布局；只保留少量测试消息，不代表可存储 7 天业务流量。

生产默认存储参数（`test` 除磁盘门槛和挂载要求外沿用相同 Kafka 默认参数）：

| 项目 | 默认值 |
| --- | --- |
| 单节点数据盘 | 512 GB HDD |
| 文件系统 | XFS 或 ext4 |
| 挂载点 | `/data1`，供 Docker 和 Kafka 持久化数据共同使用 |
| Docker data-root | `/data1/docker` |
| Kafka 部署根目录 | `/root/deploy/kafka`，位于系统盘 |
| Kafka 数据根目录 | `/data1/kafka` |
| 数据保留 | 7 天，即 `log.retention.hours=168` |
| 容量限制 | `log.retention.bytes=-1`，通过时间保留和磁盘监控控制 |
| 清理策略 | `log.cleanup.policy=delete` |
| segment 大小 | `log.segment.bytes=1073741824`（1 GiB） |
| segment 滚动 | `log.roll.hours=24`，适配低流量 Topic |
| 保留检查 | `log.retention.check.interval.ms=300000`（5 分钟） |
| 容量规划线 | 最多按 70% 规划，即每台约 358 GB |
| 磁盘告警 | 70% warning、80% critical、85% 进入人工处置 |

持久化目录建议：

```text
/root/deploy/kafka/                  # 系统盘：部署包和配置
├── controller-<n>/
│   ├── deploy.sh
│   ├── docker-compose.yml
│   ├── .env
│   ├── manifest.json
│   └── config/
└── broker-<n>/
    ├── deploy.sh
    ├── docker-compose.yml
    ├── .env
    ├── manifest.json
    └── config/

/data1/                              # 512 GB HDD：运行时持久化数据
├── docker/                          # Docker data-root
└── kafka/
    ├── controller-<n>/metadata/     # controller KRaft 元数据
    └── broker-<n>/data/             # broker 日志段和 broker 元数据
```

部署文件和持久化数据不放在同一子目录中。删除或替换 `/root/deploy/kafka/<role>-<n>/` 不应删除 `/data1/kafka/` 中的数据；停止和重新部署容器也必须保留持久化目录。

controller 与 broker 不共享数据目录，但其数据与 Docker data-root 位于同一个 `/data1` 文件系统。Docker 镜像、容器可写层和容器日志也会消耗该盘容量和 I/O，因此磁盘规划、告警和启动检查必须统计整个 `/data1`，不能只统计 broker 数据目录。

`check` 必须确认部署脚本以 root 执行、`/root/deploy/kafka` 可读写、`/data1` 是实际挂载点、总容量至少满足规格，并验证 Docker 实际 data-root 为 `/data1/docker`。同时检查 `/data1/kafka` 的容器写权限、整个 `/data1` 的使用率和基础 I/O 状态。若 `/data1` 未挂载或 Docker data-root 不符合规划，应阻止启动，避免数据写入系统根盘。

按 70% 规划线计算，每台 `/data1` 最多规划使用约 358 GB。该空间同时包含 `/data1/docker` 和 `/data1/kafka`，不能全部分配给 Kafka 消息。建议至少为 Docker 镜像、容器可写层、日志和升级临时空间预留 30～50 GB；Kafka 整个集群 7 天业务保留数据先控制在约 260～300 GB，并根据实际 Docker 占用、索引、内部主题、数据倾斜和压缩率修正。该数值用于当前低流量场景的初步判断，实际流量峰值仍需监控。

Kafka 只能删除已经关闭的 segment。低流量 Topic 如果长时间达不到 1 GiB，可能延迟清理，因此默认设置 `log.roll.hours=24`。7 天保留不是精确到期立即删除，必须保留清理周期和流量波动空间。

broker 初始容量按下式估算：

```text
单 broker 可用容量 >=
  峰值压缩后写入字节/秒 × 保留秒数 × 副本数 ÷ broker 数 ÷ 0.70
```

还需额外考虑 Docker 占用、内部主题、索引、分区倾斜、重分配临时空间和增长余量。controller 元数据目录建议至少预留 20 GiB；最终值由分区数量和变更频率评估。容量估算结果加上 Docker 预留后超过 `/data1` 的 70% 规划线时，集群初始化必须提示当前固定规格不满足需求。

## 9. 集群初始化（`cluster init`）阶段确认项

交互顺序采用“基础信息 → 拓扑网络 → 存储资源 → Kafka 策略 → 汇总确认”。缺少真实地址或 broker 数据盘容量时，只能保存草案，不生成标记为可部署的节点包。

### 9.1 基础与部署环境

| 确认项 | 默认或规则 |
| --- | --- |
| 集群名称 | 必填，用于输出目录和 Compose project name |
| 镜像 | 默认 `apache/kafka:4.3.2`；填写完整镜像地址，可使用私有 registry |
| 镜像架构 | 与目标 VM 的 amd64/arm64 匹配；实现时拉取并记录 digest |
| 分发方式 | 管理员人工复制节点包；不自动连接或修改远端主机 |
| 部署规格 | 默认 `standard`（8C32G + 512 GB HDD）；可选 `minimal`（4C16G + 512 GB HDD）、`test`（2C4G + 至少 20 GB，仅功能验证）；均默认保留 7 天 |
| 客户端认证 | `ENABLE_AUTH` 默认 false；开启后确认初始账号、密码和 `EXTERNAL_PORT`，使用 SCRAM-SHA-512，不启用 ACL/TLS |
| Docker 环境 | Linux、Docker Engine、可访问的 Docker daemon、Compose v2；可使用 `../docker/setup-docker.sh` 准备，由目标机 `check` 最终确认 |
| 日志 | stdout/stderr，Docker `json-file` 默认 50 MiB × 5 |
| 时区与 NTP | 3 台 VM 必须时间同步 |

“镜像 tag 可配置”不等于任意 Kafka 版本都已兼容。实现和验收以 4.3.2 为基线；替换版本后必须验证镜像存在、启动脚本路径、配置项和 storage format 参数。

### 9.2 拓扑、网络和目录

每个角色分别确认：

- 角色序号和全局唯一 node ID；
- 部署目标、SSH 地址和可选 SSH 用户/端口；
- 集群内通信地址；
- broker 客户端 advertised 地址；
- listener 端口及协议；
- 宿主机配置目录和数据目录；
- 目标容器 UID/GID 和目录写权限；
- 故障域标签 `broker.rack`；
- CPU、内存、JVM heap 和数据盘容量。

`cluster init` 按宿主机聚合资源和端口，验证共置时 controller 与 broker 不冲突。地址必须稳定；动态 IP 或 NAT 环境需要先确定稳定 DNS 或网络映射。

### 9.3 Kafka 关键服务配置

| 类别 | 配置项 | 初始建议与确认内容 |
| --- | --- | --- |
| 身份 | `process.roles`、`node.id`、cluster ID、directory ID | 工具生成并展示；生成后保持稳定 |
| 控制面 | `controller.quorum.bootstrap.servers`、`controller.listener.names` | 包含全部 3 个 controller 的真实地址 |
| 监听器 | `listeners`、`advertised.listeners`、`listener.security.protocol.map` | INTERNAL/CONTROLLER 固定 `PLAINTEXT`；EXTERNAL 根据认证开关映射 `PLAINTEXT` 或 `SASL_PLAINTEXT` |
| broker 通信 | `inter.broker.listener.name=INTERNAL` | INTERNAL 使用 `PLAINTEXT` |
| 数据目录 | `log.dirs`、`metadata.log.dir` | 映射到独立持久目录；broker 元数据也必须持久化 |
| 默认副本 | `default.replication.factor=3` | 适配 3 broker 集群 |
| 最小 ISR | `min.insync.replicas=2` | producer 需要配合 `acks=all` |
| offsets 主题 | `offsets.topic.replication.factor=3` | 首次创建前确定 |
| transaction 主题 | `transaction.state.log.replication.factor=3`、`transaction.state.log.min.isr=2` | 使用事务时确保单 broker 故障下仍满足设计 |
| 默认分区 | `num.partitions=3` | 只影响未显式指定分区数的新 topic |
| 自动建 topic | `auto.create.topics.enable=false` | 业务 topic 通过明确流程创建 |
| 不洁选主 | `unclean.leader.election.enable=false` | 优先避免数据丢失 |
| 保留策略 | `log.retention.hours=168`、`log.retention.bytes=-1` | 7 天仅是起点，必须结合容量确认 |
| 清理策略 | `log.cleanup.policy=delete` | compact 或 delete,compact 由具体 topic 设置 |
| 日志分段 | `log.segment.bytes=1073741824`、`log.roll.hours=24` | 低流量 Topic 每 24 小时滚动，避免 active segment 延迟过期数据清理 |
| 消息大小 | `message.max.bytes`、`replica.fetch.max.bytes` | 默认沿用目标镜像值；大消息要同步 producer/consumer/topic 配置 |
| 压缩 | `compression.type=producer` | 由 producer 选择 lz4、zstd 等算法 |
| 线程 | `num.network.threads`、`num.io.threads`、`num.replica.fetchers` | 先沿用版本默认值，压测后调整 |
| 故障域 | `broker.rack` | 有机架或可用区时填写真实值 |
| 删除 topic | `delete.topic.enable=true` | 删除仍须通过受控运维流程 |

`cluster init` 还要收集预期 topic 数、总分区数、峰值吞吐、平均消息大小、压缩率、保留时间和增长周期。这些输入用于容量提示，不用于伪造性能承诺。

副本数 3、min ISR 2 和 producer `acks=all` 共同提供预期写入语义。部署工具无法替业务客户端设置 `acks`。broker 默认值也不会覆盖已有 topic 的显式配置或自动调整已有副本布局。

### 9.4 访问控制边界

初始化通过 `ENABLE_AUTH` 选择是否启用客户端认证，默认关闭，兼容旧配置。开启时自动启用 EXTERNAL，使用 `SASL_PLAINTEXT + SCRAM-SHA-512`：

```properties
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT
inter.broker.listener.name=INTERNAL
listener.name.external.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
```

关闭认证时 EXTERNAL 使用 PLAINTEXT，是否启用由原有开关决定。内部 broker 和 controller 通信始终免认证，不需要内部账号。开启认证后，所有业务客户端（包括同内网客户端）均通过 EXTERNAL 访问，INTERNAL/CONTROLLER 只允许集群节点互访，否则可绕过认证。

初始账号在 controller 首次格式化时通过 `--add-scram` 写入，已格式化目录不会补写或更新账号。管理机生成独立客户端连接配置（含明文密码，权限 600）；controller 包仅分发派生凭据，broker 包不包含密码。Python 只使用标准库，无额外第三方依赖，目标节点无需 Python。

集群启动后，可通过 Kafka 自带 `kafka-configs.sh` 动态创建独立生产者、消费者账号及修改或删除凭据。ACL 暂不启用，已认证账号权限相同，无法限制仅管理员显式创建、修改或删除 Topic；后续可沿用这些账号配置 ACL。

所有模式均保持 `auto.create.topics.enable=false`。该设置只阻止 producer/consumer 触发自动创建，不阻止 CLI 或 Admin API 显式创建。当前依靠网络隔离、业务操作约束和受控运维流程管理 Topic。

完整初始化、账号管理、客户端配置及已有集群切换步骤见 [账号认证说明](authentication.md)。

## 10. Compose 与容器启动设计

每个角色包只有一个 Kafka service，关键约定如下：

- `network_mode: host`；
- `restart: unless-stopped`；
- 显式设置 `cpus`、`mem_limit`、`stop_grace_period: 120s` 和 `nofile`；
- 数据目录读写挂载，Kafka 配置只读挂载；
- JVM 参数通过 `.env` 注入，`KAFKA_HEAP_OPTS` 使用相同的 Xms/Xmx；
- Docker 日志启用大小和文件数轮转；
- Compose project name 和容器名包含集群、角色及序号；
- 不把健康检查简化为单纯 TCP 探测。

服务显式使用挂载后的 `server.properties` 启动，避免镜像入口根据环境变量再次生成一份配置。实现前必须用 4.3.2 实际镜像确认：

1. Kafka CLI 和启动脚本路径；
2. 镜像运行 UID/GID 及目录权限；
3. 官方 entrypoint 对 command/环境变量的处理；
4. PID 1 信号转发和 120 秒优雅退出；
5. amd64/arm64 支持；
6. 挂载配置不会被入口脚本覆盖。

确认这些行为后再固定 Compose command/entrypoint，方案阶段不写死未经镜像验证的内部路径。

## 11. 节点脚本行为

### `check`

只读检查并汇总：

- 节点包文件和 SHA-256；
- `docker` 命令存在，Docker daemon 正常且当前用户具有访问权限；
- `docker compose` V2 可用，Compose 文件可通过 `docker compose config --quiet`；
- Docker server 架构与目标 Kafka 镜像匹配；
- Docker data-root 路径、所在挂载点及剩余空间符合规划；
- 镜像可用性，可拉取或已离线加载；
- CPU、内存、文件描述符、时间同步；
- 端口占用；
- 目录路径、权限、剩余空间和实际挂载点；
- 已有 `meta.properties` 与节点包身份是否一致；
- 到其他角色地址的连通性和 listener 的网络访问边界。

本节是 Kafka 节点脚本的实现约束，包含尚需完善的检查项；当前 `deploy.sh check` 执行文件摘要、Docker/Compose、路径、磁盘和已有身份检查。静态校验不运行目标机检查；本地 `test` 验证已在 3 个隔离 Linux 节点中实际执行 `check`、`prepare` 和生命周期命令。

### `prepare`

先完整执行 `check`，然后创建目录并按第 6 节进行幂等格式化。`prepare` 不启动长期运行的 Kafka 服务，也不覆盖已存在且身份不一致的数据。

`prepare` 从实际镜像解析运行 UID/GID，并设置挂载配置目录、配置文件和数据目录的所有者，避免 root 生成的受限文件使容器用户无法读取或写入。storage format 使用 128–256 MiB 的临时 CLI heap，成功后必须存在 `meta.properties`。重复 `prepare` 仍检查身份，只修复挂载目录权限并跳过格式化。

### `start`

再次检查身份和配置摘要，执行 `docker compose up -d`，再用角色对应的 Kafka CLI/API 检查本节点状态。controller 在 quorum 尚未形成时可以报告“进程已启动、等待 quorum”，但不能报告集群健康。

### `stop` 与 `restart`

`stop` 使用 Compose 正常停止并保留数据。`restart` 只用于单节点操作；集群滚动重启由运维流程逐节点执行，每次等待 quorum 或 ISR 恢复后再处理下一节点。

### `status` 与 `logs`

`status` 同时显示 Compose 状态、本地进程状态和角色 API 状态。`logs` 支持可选 service 参数和 `--tail`，默认跟随当前角色日志。

节点脚本不提供删除数据或重新生成身份的命令。

## 12. 分发设计

默认由管理员人工复制节点包：

```bash
scp -r cluster/nodes/controller-1-<host>/ root@<host>:/root/deploy/kafka/controller-1/
scp -r cluster/nodes/broker-1-<host>/     root@<host>:/root/deploy/kafka/broker-1/
```

节点包默认由管理员人工分发。本轮不实现自动 SSH/scp 分发，避免集群初始化工具隐式连接或修改远端主机。分发时使用 SSH host key 校验，并将每个角色包复制到新的目标目录；不要覆盖正在运行的部署目录，也不要操作 `/data1/kafka` 数据目录。

镜像不可在线拉取时，可在 `cluster init` 指定已由 `docker save` 产生的镜像包。生成工具记录文件摘要并按需放入节点包，目标机仅在镜像不存在时执行 `docker load`。是否携带大型镜像包必须显式确认，避免 6 份重复副本占满管理机磁盘。

## 13. 启动、变更与恢复规则

### 首次启动

1. 所有角色先完成 `check` 和 `prepare`。
2. 启动 controller-1 和 controller-2，使 quorum 达到多数派。
3. 确认选出 leader 后启动 controller-3，并等待追平。
4. 依次启动 3 个 broker，并确认全部注册。
5. 部署后按第 14.2 节执行完整集群验收。

### 配置变更

- 先重新渲染并展示差异；
- 按 Kafka 配置的动态/静态属性决定在线修改或重启；
- 需要重启时逐节点执行，controller 与 broker 每次只处理一个；
- 等待 quorum、broker 注册和 ISR 恢复后再继续；
- 不通过批量 `docker compose restart` 同时重启 3 个 controller。

### 镜像升级

修改 tag 不等于完成升级。升级前必须核对 Kafka 官方升级路径、协议兼容、metadata version、配置废弃项和回滚限制。先备份配置与身份清单，按滚动方式升级；metadata feature 升级作为独立且可能不可逆的步骤，不能由普通 `start` 隐式执行。

### 故障恢复

- broker 数据盘故障：先隔离故障节点，确认副本健康，再按 Kafka 副本恢复流程处理；脚本不清空目录。
- controller 节点替换：使用目标版本规定的 quorum 成员变更流程，保留或重新分配明确的 directory ID；不重新初始化整个 quorum。
- 身份冲突：停止该节点，比较 manifest 与 `meta.properties`，由管理员确认正确身份后修复。
- 磁盘满：先限制写入和恢复空间，不直接删除 Kafka 管理中的 segment 文件。

## 14. 集群验证与验收

### 14.1 配置生成验收

- 生成 3 个 controller 和 3 个 broker 独立节点包；
- 共置三 VM 和角色分置六 VM 两种拓扑均能正确渲染；
- node ID、cluster ID、directory ID、地址、端口和目录无冲突；
- `docker compose config --quiet`、`bash -n` 和模板变量残留检查通过；
- 重复执行 `cluster init` 时保留集群身份；错误输入不会覆盖已有生成包；
- 敏感信息不会出现在控制台摘要、普通日志或 Git 跟踪文件中。

### 14.2 运行验收

- quorum 有 leader，3 个 controller 均为 voter 且复制状态正常；
- 3 个 broker 全部注册；
- 从实际客户端网络访问每个 advertised listener；
- 创建验收 topic：3 分区、3 副本、min ISR 2；
- producer 使用 `acks=all`，完成生产和消费校验；
- 验证访问不存在的 Topic 不会自动创建，同时验证管理员可以通过 CLI 显式创建验收 Topic；
- 停止一台共置 VM 后，quorum 保持多数派，读写符合副本和 ISR 设计；
- 恢复节点后等待 quorum 和 ISR 追平；
- 停止两个 broker 时，写入按 min ISR 预期受限；
- 容器重建和宿主机重启后 cluster ID、元数据和消息保持不变；
- 重复 `prepare` 不重新格式化；
- 配置摘要或身份不一致时拒绝启动。

### 14.3 测试规格的最小验收

本地 `test` 只要求验证部署脚本功能和部署后的 Kafka 基础行为，不执行第 14.4 节的性能验收，也不要求完整的生产故障演练：

1. 3 台 VM 上的 6 个角色包均能完成 `check`、`prepare`、`start`、`status` 和 `stop`。
2. quorum 有 leader，3 个 controller 为 voter 且复制正常，3 个 broker 注册。
3. 显式创建 3 分区、3 副本、min ISR 2 的测试 Topic，确认每个分区 ISR 为 3。
4. 以 `acks=all` 发送少量消息并消费比对；访问不存在的 Topic 不会触发自动创建。
5. 重启单个 broker，等待 ISR 恢复后再次消费已有消息；重复 `prepare` 跳过格式化，身份保持不变。

命令示例见 [本地最小功能验证](local-test.md)。静态生成校验不能证明上述运行验收已经通过。

### 14.4 性能验收

功能验收不能替代容量验收。上线前使用接近真实的消息大小、压缩算法、分区数、producer 并发、consumer 并发和保留策略压测，并记录：

- 峰值与持续吞吐；
- produce/fetch 延迟；
- request queue 和网络/磁盘利用率；
- JVM GC 和堆外内存；
- page cache、磁盘时延和磁盘增长；
- replication lag、under-replicated partitions 和 ISR 变化。

压测结果用于调整线程数、JVM heap、磁盘容量和 topic 分区，不在部署脚本中固化未经验证的“高性能参数”。

## 15. 实现边界

实现范围包括：

- 集群初始化交互、配置校验和 6 个角色包生成；
- 节点本地检查、幂等 prepare 和 Compose 生命周期；
- 可选的安全 scp 分发；
- quorum、broker 和基础读写验证。

本方案不负责：

- 自动修改云安全组、防火墙、DNS、NTP、路由或 registry 权限；
- 自动创建所有业务 topic，或管理 producer 和 consumer 配置；
- 在未启用 ACL 时限制管理员与普通客户端的操作权限；
- 自动扩缩 controller/broker 或自动数据重分配；
- 跨集群复制、灾备集群、MirrorMaker 2；
- 监控平台和告警平台本身的部署；
- 未经验证的 Kafka 版本升级和 metadata feature 升级；
- 自动删除数据或回收正式集群。

## 16. 实现顺序

当前实现和验证顺序：

1. 已建立 `kafka-admin.sh`、配置层、公共库和校验器。
2. 已实现身份 manifest 和可重复的 `cluster init`。
3. 已实现 properties、Compose 和角色包模板。
4. 已实现节点 `check`、`prepare`、`start/stop/restart/status/logs`。
5. 支持标准/最小生产/测试三档规格的节点包生成、Shell 语法、JSON、文件摘要和 Compose 静态校验。
6. 已使用实际 arm64 `apache/kafka:4.3.2` 镜像，在 3 个隔离 Linux 节点中执行 `test` 档的节点检查、storage format、启动、quorum、broker、3 副本生产消费、幂等 prepare、broker 重启和容器重建后的消息保留验证；发现并修复镜像用户无权读取 root 配置的问题。
7. 独立 VM、amd64、完整生产故障场景、信号退出时序和容量验收仍未执行。本地功能通过不代表生产部署验收通过。

生产部署仍需完成目标环境的检查和第 14 节相应验收。
