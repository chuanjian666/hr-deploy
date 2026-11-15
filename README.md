# hr-deploy

用于准备 Docker 基础环境，以及生成、部署和验证 Kafka KRaft 集群的脚本与操作文档。

- **Docker**：支持在线／离线安装 Docker Engine 和 Compose V2，配置存储目录及日志轮转。
- **Kafka**：生成 3 个 controller 和 3 个 broker 的独立 Compose 节点包，支持 `standard`、`minimal`、`manual` 资源规格及可选的 SCRAM 客户端认证。

## 仓库结构

```text
docker/             Docker 安装配置脚本、使用说明和测试
kafka/
├── kafka-admin.sh  管理机入口
├── config/         管理配置及示例
├── scripts/        配置校验、部署包生成及认证逻辑
├── templates/      节点部署脚本和 Compose 模板
├── docs/           部署、认证及验收文档
└── tests/          脚本测试和本地集成验证工具
```

## 环境与执行位置

- **管理机**：用于生成和校验节点包，需要 Bash、Python 3、OpenSSL、base64、`sha256sum`；分发节点包还需 SSH 和 scp。
- **目标节点**：三台互通的 Linux 服务器，通常每台部署一个 controller 和一个 broker；需要 Docker Engine、Compose V2 及相关系统工具，节点检查以 root 执行，无需 Python。
- **存储与网络**：生产规格要求 Kafka 数据所在文件系统独立挂载；容量、端口和资源要求见[生产部署指南](kafka/docs/deploy-production.md)。Docker 与 Kafka 数据目录分别配置。

## 部署流程

1. 在每台目标节点按 [Docker 使用说明](docker/README.md)准备 Docker 环境。
2. 在管理机的仓库根目录执行初始化及静态校验：

   ```bash
   cd kafka
   ./kafka-admin.sh cluster init
   ./kafka-admin.sh cluster validate
   ```

3. 将 `kafka/cluster/nodes/` 下的节点包分发到对应服务器，在各角色目录执行 `deploy.sh check` 和 `deploy.sh prepare`。
4. 启动全部 controller，执行 `deploy.sh health 120` 并通过健康检查后，再启动 broker。
5. 按[消息收发验证](kafka/docs/verify-messages.md)检查集群及生产消费结果。

完整步骤见[本地部署](kafka/docs/deploy-local.md)或[生产部署](kafka/docs/deploy-production.md)。

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [Docker 使用说明](docker/README.md) | 安装方式、存储配置及已有 Docker 的重新配置 |
| [Kafka 使用说明](kafka/README.md) | 初始化、节点操作、资源规格及存储目录 |
| [本地部署](kafka/docs/deploy-local.md) | 本地模拟节点的手动部署流程 |
| [生产部署](kafka/docs/deploy-production.md) | 三台 Linux 服务器的部署与验收 |
| [账号认证](kafka/docs/authentication.md) | SCRAM 配置、账号管理及已有集群切换 |
| [部署后启用 ACL](kafka/docs/enable-acl-after-deploy.md) | 权限切换、账号授权、正反向验证及回退 |
| [消息收发验证](kafka/docs/verify-messages.md) | Topic、生产、消费及内容比对 |
| [功能验证要求](kafka/docs/local-test.md) | 测试环境、自动化入口和验收记录 |

## 测试与验证

- **静态校验**：`cluster validate` 校验生成产物，不拉取镜像或启动容器。
- **脚本测试**：[Docker 配置](docker/tests/test-config.py)、[Kafka 基础功能](kafka/tests/test-basic.sh)、[controller 健康检查](kafka/tests/test-controller-health.py)。
- **集成验证**：[认证冒烟测试](kafka/tests/test-auth.py)需要 Docker；[本地自动验证](kafka/tests/test-local.sh)使用 privileged Docker-in-Docker 模拟三个节点，环境及执行步骤见[功能验证要求](kafka/docs/local-test.md)。

## 操作注意事项

- Docker 安装配置脚本会修改 `daemon.json` 并重启服务；重新配置 data-root 不会迁移旧数据。
- Kafka `prepare` 用于首次格式化数据目录；已有且身份一致的目录会跳过，身份冲突或非空未格式化目录会被拒绝。
- 客户端认证默认关闭；启用后使用 `SASL_PLAINTEXT + SCRAM-SHA-512`，内部通信始终为 PLAINTEXT。当前未启用 TLS 和 ACL，需限制内部端口的访问范围，详见[账号认证](kafka/docs/authentication.md)。
- 管理配置和生成的 `kafka/cluster/` 可能包含凭据，应妥善保管；容器 Running 或静态校验通过不能替代运行验收。
