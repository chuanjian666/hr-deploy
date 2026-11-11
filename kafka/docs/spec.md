
## 部署 kafka 集群

- 阶段过程：集群初始化（`cluster init`）阶段：初始化集群部署（交互的方式确认部署信息，生成每个节点的部署脚本）；deploy 阶段：拷贝部署脚本到节点（可以是手动，也可以安装脚本支持 scp 拷贝到指定节点）；启动服务：在指定节点上启动服务

- 版本选择：默认版本是 4.3.2（目前最新 release 版的上一个版本），也支持部署时生成配置时指定镜像版本

- 集群规模：3 节点

- 部署方式：使用 docker compose 部署

- 部署角色：controller 和 broker 拆分部署（有各自独立的部署脚本、可以运行在同一台 vm，也可以拆分部署，看把部署脚本如何方式）

- 资源与磁盘规格：集群初始化时从三个固定档位中选择。标准规格 `standard` 为单台 8C32G，最小生产规格 `minimal` 为单台 4C16G；两者均默认配置每台一块 512 GB HDD，挂载到 `/data1`，供 Docker data-root 和 Kafka 持久化数据使用。新增测试规格 `test` 为单台 2C4G、至少 20 GB 磁盘，允许 `/data1` 使用系统盘目录，无需独立数据盘，Docker data-root 仍为 `/data1/docker`。三种规格的 Kafka 部署包和配置统一放在 `/root/deploy/kafka`，默认保留 7 天数据，均为 OS 和 Docker 预留资源。最小生产规格只适用于 HR 内部系统等低吞吐、小分区规模场景，并要求上线前压测。

- 测试规格边界：仅在本地 3 台 Linux VM 中验证部署脚本和部署后的 Kafka 基础功能，每台共置一个独立 controller 和一个独立 broker。不用于生产，不涉及性能测试，只写入少量消息；验证 quorum、3 个 broker、显式创建 Topic、3 副本/min ISR 2 下 `acks=all` 生产消费、重启后数据保留及重复 prepare 的幂等性。资源预算不是已验证的运行或容量承诺。

- 部署配置：部署相关的环境变量保存到 .env

- 服务配置：服务本身的配置也挂载出来（你先生成部署方案，列出来 Kafka 哪些关键配置需要在集群初始化（`cluster init`）阶段确认）

## 总结

先按照上述需求给出方案，先不着急实现

## 需求补充

- 当前部署在可信内网；初始化时可选择是否启用客户端 SCRAM-SHA-512 账号认证，默认关闭。开启后使用独立 EXTERNAL 入口（SASL_PLAINTEXT），broker 与 KRaft 内部通信保持免认证。SSL/TLS 不启用，ACL 后续再配置。
- 设置 `auto.create.topics.enable=false`，关闭 producer/consumer 访问不存在 Topic 时的自动创建。
- Topic 由管理员通过 Kafka CLI 或 Admin API 显式创建；没有 ACL 时 Kafka 无法阻止其他可访问客户端显式创建 Topic，该限制依赖网络和运维流程。生产者和消费者可分别使用独立账号，后续可沿用账号配置 ACL。
