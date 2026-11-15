# 测试环境功能验证要求

## 资源与范围

本节以 `manual` 手动配置下表资源，用于验证部署脚本和 Kafka 基础功能，不用于生产或性能测试。准备 3 台互通的 Linux VM，每台 2 vCPU / 4 GiB 内存，磁盘容量按系统、镜像和少量测试消息的实际占用规划，不设规格下限；合计 6 vCPU / 12 GiB 内存，宿主机另需保留自身资源。每台 VM 共置一个 controller 和一个 broker，仍生成 6 个独立角色包。验收应由执行人员在目标环境中完成并另行留档。本文不记录部署实例，执行目录约定见 [本地执行目录](variables.md)。

| 每台节点用途 | CPU limit | memory limit | JVM heap |
| --- | --- | --- | --- |
| controller | 0.5 vCPU | 512 MiB | 256 MiB |
| broker | 1 vCPU | 1536 MiB | 512 MiB |
| OS、Docker 与临时 CLI 预留 | 0.5 vCPU | 2 GiB | — |

## 三台 Linux VM 手动验证

每台 VM 创建 `/data1` 目录并使用 [Docker 安装脚本](../../docker/README.md)准备 Docker Engine 和 Compose v2，Docker data-root 仍为 `/data1/docker`。`manual` 允许 `/data1` 是系统盘目录，无需独立挂载；部署检查时至少需要 5 GB 可用空间。本方案的 host 网络要求 3 台 Linux VM，不能将 3 个地址全部填写为同一个本地地址来代替三节点。

### 生成部署包

在仓库 `kafka/` 目录执行，按提示选择 `manual`，依次填写每个 controller、broker 的 CPU 和内存（本节示例为 `0.5`、`512m`、`1`、`1536m`），再输入实际 VM 地址：

```bash
./kafka-admin.sh cluster init
./kafka-admin.sh cluster validate
```

节点包统一生成在 `cluster/nodes/`。`manual` 使用输入的容器资源上限并允许系统盘目录，仍保留 3 controller、3 broker、3 副本、min ISR 2、关闭 Topic 自动创建及身份与文件摘要校验。配置默认值仍为 `standard`；所有档位均不限制磁盘总容量。当前 DinD 环境创建的 20 GiB 测试盘属于测试环境设置，不是规格下限。

### 部署与基础验收

按 [节点操作](../README.md#节点操作) 分发部署包，在 6 个角色包中执行 `check`、`prepare`，先启动 controller 再启动 broker。部署后按 [消息收发验证](verify-messages.md) 执行通用检查，controller 可通过 `./deploy.sh health 120` 检查。验收以 quorum、broker、ISR 和读写结果为准，容器 Running 不等于业务可用。重复 `prepare` 应保持身份一致；重启及容器重建后的消息保留检查仅在安排好的测试环境执行。

## 测试宿主机自动验证

可使用仓库的 Docker-in-Docker 模拟节点。测试镜像、资源、磁盘文件和网络以 `tests/local/` 配置为准；节点使用 privileged 容器并共享宿主内核，不等同于三台独立 VM。

在测试宿主机终端进入仓库根目录，交互初始化时选择 `manual`，填写上表示例资源。controller 和 broker 地址均填写 `172.30.93.11,172.30.93.12,172.30.93.13`。

```bash
cd /path/to/hr-deploy
"$PWD/kafka/kafka-admin.sh" cluster init
"$PWD/kafka/kafka-admin.sh" cluster validate
bash "$PWD/kafka/tests/test-local.sh" up
bash "$PWD/kafka/tests/test-local.sh" prepare
bash "$PWD/kafka/tests/test-local.sh" start
bash "$PWD/kafka/tests/test-local.sh" verify
bash "$PWD/kafka/tests/test-local.sh" status
# 验证后停止，保留测试盘及日志。
bash "$PWD/kafka/tests/test-local.sh" stop
```

自动化工具读取仓库 `kafka/cluster/` 中已生成的节点包，日志和 `result.json` 写入 `kafka/generated/local-test/`。只有验证成功退出且本轮结果为 `status=passed` 才判定通过。完整自动验收会更新测试账号并重启、重建 broker。

### 启用认证的自动验证

首次初始化时选择启用认证并输入初始账号；首次创建可将 `AUTH_PASSWORD` 留空自动生成 6 位随机密码，也可手动指定密码（不限制长度，保留字符集要求），配置文件权限保持 `600`。本地验证按首次初始化流程执行；修改配置或重复 `prepare` 不会改写已有账号。

`verify` 会根据生成清单选择客户端端口和协议。开启认证时，读取管理机 `cluster/clients/admin.properties`，验证三个 broker 的认证入口、错误密码和无凭据连接被拒绝、INTERNAL 保持免认证，并创建独立的 `hr_producer` / `hr_consumer` 账号完成生产消费。业务账号密码随机生成，客户端文件保存在 `cluster/clients/`，权限 `600`；真实密码不写入命令日志。再次执行验收会更新这两个测试账号的密码。

认证模式同样验证 controller quorum、复制、资源限制、关闭自动创建 Topic、重复 prepare 幂等，以及 broker 重启和容器重建后的消息保留。CLI 的临时连接配置会在容器重建后重新装入，不依赖容器内临时文件持久化。

## 验收记录

执行人员应在受控的环境验收记录中保存执行时间、镜像版本和架构、集群身份、检查结果及日志位置；这些实例值不写入通用仓库文档。验证命令成功退出且本轮新结果为 `status=passed` 才算通过，避免误读已有结果。
