# PostgreSQL 主库迁移与性能验收

当前验收脚本使用课程库的 14 家商铺。文中的万店测量和执行计划归档自旧版本，当前脚本用于小样本功能对照。


运行时使用 PostgreSQL 17、PostGIS 3.5、pgx 和 GORM。登录、商铺、笔记、Feed、预约、签到、订单、支付、核销和后台任务共用同一个 PostgreSQL 连接池。Redis 继续承载缓存、会话及秒杀受理，Kafka 继续承载订单消息。历史 MySQL 驱动只供一次性导入工具与跨库基准使用。

## 首次部署

```sh
docker compose up -d --build --wait
curl -fsS http://localhost:8081/health/ready
```

空库在首次执行基础迁移时可加载演示数据。Compose 默认开启。部署需要空业务库或导入已有数据时，使用 `XZDP_SEED_DEMO=false`。PostgreSQL 的连接变量为 `XZDP_POSTGRES_DSN`，Compose 通过 `POSTGRES_PASSWORD` 配置数据库，API 容器通过 `PGPASSWORD` 连接。迁移在数据库事务内执行，以事务级 advisory lock 协调多个实例；首次安装需有创建 postgis 和 btree_gist 扩展的权限。[Schema 与索引](../internal/migrate/pg)

## 从已有 MySQL 切换

1. 保留原版本的应用镜像、配置和 MySQL 数据卷。先使用另一个空 PG 数据库演练导入。正式切换时停旧 API、消费者及其他写入者，将源 MySQL 设为只读，再复制最终快照。
2. 仅启动新库并构建 API 镜像。将能访问源 MySQL 的 DSN 放入环境变量 `XZDP_SOURCE_MYSQL_DSN`。历史 Compose 的源主机通常为同网络的 `mysql:3306`，示例格式为 `user:password@tcp(mysql:3306)/xzdp`。目标 PG 需使用新数据库或空业务表。
3. 在仓库根目录运行以下命令，确认导入进程成功退出，再启动 API。工具从可重复读快照读取源表，在一个 PG 事务中写入、比较行数、逐主键顺序比较全部原始字段的 SHA-256，并按原自增高水位重设 identity 序列。地理点由坐标自动生成。业务表含已有行、字段不匹配或校验失败都会终止导入，原有目标业务行不会被覆盖。

```sh
docker compose up -d --wait postgres
docker compose build api
docker compose run --rm --no-deps -e XZDP_SOURCE_MYSQL_DSN --entrypoint /app/dbcopy api
XZDP_SEED_DEMO=false docker compose up -d --wait
curl -fsS http://localhost:8081/health/ready
```

4. 核对商铺详情、附近搜索、热门榜和订单状态，再停止旧 MySQL。Redis、Kafka 和上传文件沿用原来的存储。新系统开始接受写入后，回切旧库前需要处理切换点之后的新数据。日常备份和恢复使用 pg_dump、pg_restore 及 Redis RDB。[导入工具](../cmd/dbcopy/main.go) · [恢复演练](recovery.md)

本机实际迁入 25 张业务表，共 5,379 行，含 1,006 名用户、1,525 笔订单及对应的 1,525 条消费回执。最终导入前停止了旧 API 并将 MySQL 设为只读。旧容器已停止，原卷为 `xzdp_mysql-data`。从切换后运行中的 PG 和 Redis 重新制作快照，并在独立容器恢复，恢复库中仍有 1,525 笔订单。

## 找店与落单的优化

- 无关键词和分类的附近首页走 PostGIS GiST 的 KNN 距离排序。先找最近的 10 家并保留同距候选，再按半径过滤、按 ID 确定边界次序。在 24,000 店的样本计划中，数据库沿距离索引读了 11 行。
- 分类、关键词和分页先缩小候选，再对符合条件的店排序。简单查询省去空关键词的小写转换与子串检查。优化期间尝试了对关键词也强制距离优先，实测其 P95 升至 1.51 ms，故舍弃。保留普通空间索引、分类 B-tree 及分类与位置的组合 GiST，由优化器按参数选择。实际反复执行的参数化关键词请求使用了组合 GiST，样本中索引扫描返回 11 行。PostGIS 球面距离转换为原接口使用的半径，测试覆盖同址超过一页、百分号、重音和半径边界。[查询实现](../internal/discovery/search.go)
- Kafka 消费者用一次 PG 调用完成回执去重、订单写入、条件扣库存和通知写入。整次调用是一个原子事务，库存耗尽会同时撤销回执及订单。重复消息直接复用已有处理结果。这减少了事务占用连接及热点库存锁期间的数据库往返。[落库函数](../internal/migrate/pg/002_order.sql)

## 优化前后的本地数据

两组均为 PostgreSQL 17.6 / PostGIS 3.5.3 和同一 pgx 驱动，数据库限 2 CPU、512 MiB，缓冲区 96 MiB，默认同步提交。对照使用已建 GiST 的旧版 PG 搜索 SQL 和多条 SQL 的落单事务。在三个独立的临时数据库中，各用 24,000 家合成店、8 并发，预热后交替跑 6 轮，每轮 480 次。表中取 18 轮指标的中位数。搜索走本机 Hertz HTTP，落单测数据库持久化阶段。下表与旧报告的机器负载、驱动和事务表结构存在差异，请在同一行内比较。

| 场景 | P50 前 → 后 | P95 前 → 后 | 完成速率前 → 后 |
|---|---:|---:|---:|
| 附近首页 | 0.80 → 0.71 ms | 1.275 → 1.062 ms | 9,424 → 10,622 次/秒 |
| 分类 + 关键词 | 0.70 → 0.60 ms | 1.000 → 0.835 ms | 11,053 → 12,826 次/秒 |
| 订单落库事务 | 3.64 → 0.91 ms | 8.739 → 2.497 ms | 1,861 → 7,317 笔/秒 |

独立业务验收中，1,000 人争抢 100 张券，产生 100 笔订单、100 个不同买家，剩余库存为零。支付／核销并发重试、订单过期、候补转正、死信恢复、Feed、点赞、签到和邮件绑定均通过。压测中的数据库吞吐不能直接当作整个秒杀 API 的吞吐。[逐轮数据](performance/postgres-tuning.csv) · [实际执行计划](performance/postgres-tuning-plans.txt)

```sh
go test ./...
go vet ./...
sh script/pgbench/production.sh
python3 script/load/sale.py
python3 script/recovery/drill.py drill
```

生产负载的长尾延迟仍取决于实际事务组合、网络、磁盘持久性及活动热点分布。本次测量使用单机单实例。[历史 MySQL／PG 对照](postgres-evaluation.md)
