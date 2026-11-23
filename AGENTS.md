# AGENTS.md — 小众点评 (xzdp-go)

本文档说明 **xzdp-go** 的模块职责、开发约定和常用验证命令，方便开发者与 AI 编程助手快速上手。

## 1. 模块职责

| 路径 | 职责 |
|---|---|
| `cmd/api/` | Hertz HTTP 服务入口：提供全部业务接口、内嵌 Vue 3 前端、`/livez`、`/readyz`、`/metrics` 与平滑停机 |
| `cmd/worker/` | 后台消费者入口：通过 Redis Streams 消费者组异步落库秒杀订单、接管崩溃消息、关闭超时订单并回补库存 |
| `cmd/bench/` | 压测与真实环境集成测试：验证多级缓存击穿保护、单桶/八桶秒杀一致性与接管、PostGIS 附近商铺搜索 |
| `internal/httpapi/` | Hertz 路由注册、鉴权/限流/UV 中间件、内嵌静态资源（`embed.FS`）与 `SIGTERM` 停机摘流 |
| `internal/cache/` | 商铺多级缓存：进程内有界 LRU（L1）+ Redis（L2）+ `singleflight` 回源合并 + 逻辑过期异步刷新 + 15 秒空值防穿透 |
| `internal/trade/` | 优惠券与秒杀核心：预编译 Lua 脚本原子限购/扣库存/写 Stream、分桶库存、消费者组落库、支付、核销、超时关单与候补递补 |
| `internal/shop/` | 商铺详情、分类、PostGIS `ST_DWithin` 附近找店与门店幂等预约 |
| `internal/social/` | 探店笔记、点赞、评论、关注与关注流 |
| `internal/user/` | 邮箱/短信验证码登录、会话管理与 Redis BitMap 月度签到领券 |
| `internal/store/` | `pgx/v5` 连接池与内嵌 SQL 迁移脚本（`internal/store/pg/001–004` + `demo.sql`） |
| `internal/metrics/` | 低基数 Prometheus 指标定义（路由模板与固定 Stream 操作枚举） |
| `frontend/` | Vue 3 + TypeScript + Tailwind CSS v4 暗色控制台（构建产物输出到 `internal/httpapi/web/`） |

## 2. 核心开发规则

1. **保持极简依赖与纯 Go Hertz 写法**
   - 保持现有 5 个核心运行时依赖（`hertz`、`pgx/v5`、`go-redis/v9`、`prometheus/client_golang`、`golang.org/x/sync`），不重新引入 Thrift IDL、`hz` 代码生成、GORM、Kafka 或 MySQL 驱动。
   - 日志统一使用标准库 `log/slog`，配置通过环境变量传入，禁止使用全局业务单例。
2. **秒杀原子性与幂等落库不可破坏**
   - 抢购限购校验、库存扣减与 `XADD` 写入 Redis Stream 必须在同一个预编译 Lua 脚本（`reserveScript`）中完成。
   - 所有高频 Redis Lua 脚本（限流、预热、抢购、候补、释放库存、消息确认）必须定义为包级 `*redis.Script` 走 `EVALSHA` 缓存。
   - Worker 落库必须保持 `ON CONFLICT (id) DO NOTHING` 幂等语义，数据库事务提交成功后再调用 `finishMessage` 执行 `XACK`/`XDEL`；保留 `XAUTOCLAIM` 定时接管与超时未支付订单自动关单回补库存逻辑。
3. **监控低基数、平滑停机与前后端同步**
   - Prometheus 指标标签只允许路由模板和固定枚举（`ack` / `reclaim` / `dead_letter`），严禁放入 `shop_id`、`user_id`、`order_id` 等高基数字段。
   - 收到 `SIGTERM` 后必须先置位 `draining` 让 `/readyz` 立刻返回 503，再调用 `h.Shutdown` 等待存量请求完成并停止 Worker。
   - 修改 `frontend/` 后必须运行 `make frontend` 更新 `internal/httpapi/web/` 内嵌静态资源。
4. **数据库迁移与仓库整洁**
   - `internal/store/pg/` 下的 SQL 迁移按编号只增不改。
   - 不提交 `.env`、`.DS_Store` 或临时产物；文档中的所有性能数字必须与 `docs/benchmarks-raw.json` 保持一致。

## 3. 常用验证命令

```sh
gofmt -l cmd internal
go vet ./...
go test -race -count=1 ./...
make frontend
make bench
docker compose config -q
```

如需运行连接真实 PostGIS 与 Redis 的全量集成测试，启动依赖后设置 `XZDP_INTEGRATION=1`、`DATABASE_URL` 和 `REDIS_URL` 执行 `go test -race -count=1 ./...`。
