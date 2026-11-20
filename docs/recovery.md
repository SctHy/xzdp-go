# 本地备份与隔离恢复演练

需要 Python 3.11+、Docker Compose，以及当前用户对本项目容器的访问权限。在仓库根目录运行：

```sh
python3 script/recovery/drill.py drill
```

脚本读取当前 Compose 的 PostgreSQL 与 Redis，导出包含触发器的 PostgreSQL 自定义格式备份 和 Redis RDB，计算 SHA-256。它创建临时 PostgreSQL、Redis 两个容器，使用独立数据卷，关闭网络，不发布端口。它导入 SQL，查询商铺、订单和迁移记录；确认新 Redis 为空，再从 RDB 重启，检查键和点赞成员。已有数据不会被导回当前工作栈。临时容器及其数据卷在退出时清理。

需要留存快照或重复验证时：

```sh
python3 script/recovery/drill.py backup "$HOME/xzdp-backup-01"
python3 script/recovery/drill.py verify "$HOME/xzdp-backup-01"
```

备份目录须为新目录，权限为 0700，其中包含真实会话和业务数据，请存放在私有位置。导出分别取两个存储的快照；若要以同一个业务时点恢复，请先暂停所有应用实例及其他写入方，备份完成后再恢复流量。PostgreSQL 可以重建商铺坐标索引；点赞成员、排队占位与候补顺序还依赖 Redis。本演练覆盖这两种存储，Kafka 日志、上传图片及跨节点灾备需分别备份验证。
