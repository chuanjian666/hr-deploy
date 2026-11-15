# 本地执行目录约定

本地命令在仓库根目录执行，直接使用 Shell 自带的 `$PWD` 获取当前目录。

```bash
cd /path/to/hr-deploy
```

将示例路径替换为实际仓库路径，后续本机命令保持在该目录执行。新终端同样先进入仓库根目录。进入模拟节点后，按文档使用节点内的固定绝对路径。

本地模拟节点固定为 `172.30.93.11`、`172.30.93.12`、`172.30.93.13`，容器名固定为 `hr-kafka-local-test-node-1-1`、`hr-kafka-local-test-node-2-1`、`hr-kafka-local-test-node-3-1`。

管理配置保存在仓库的 `kafka/config/kafka-admin.conf`，生成包保存在 `kafka/cluster/`。目标节点的角色包直接使用 `/root/deploy/kafka/controller` 和 `/root/deploy/kafka/broker`。

生产部署命令直接写出路径和示例 IP，执行前按实际环境替换。
