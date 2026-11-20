# Phase 1 商铺读缓存

商铺详情和商铺分类沿用原 HTTP 接口与响应格式。数据库无需迁移。分类在两级缓存中都是一个 JSON 数组。freecache 的默认容量为每个 API 实例 16 MiB。

## 请求与故障处理

L1 未命中时，同节点同一键的请求进入 singleflight。共享任务再次查询 L1，然后查 Redis。L2 未命中时用 SET NX 获取 4 秒租约，持有者再次检查 Redis 后回源。Lua 脚本校验租约 token，再写缓存并释放租约。每个调用方可以自行取消等待，共享任务有独立的 3 秒超时。Redis 无法连接时各节点独立回源并填充 L1。

默认 L1 有效期 30 秒，L2 为 600 秒加 ±20% 抖动。数据库确认不存在的商铺在两级缓存中保存 null，最多 3 秒。其他 DB 错误直接返回调用方。配置见各环境 YAML 的 shop_cache 段；hot_ids 默认包含商铺 1。启动时以及每隔 60 秒刷新 ID 过滤器，同时访问分类和配置的热点商铺以预热。

Bloom Filter 在完整读取商铺 ID 后原子替换。默认模式下，过滤器判断不存在且两级缓存未命中时，只做主键 EXISTS 检查；新建商铺因此仍能被发现。对从未查询过的大量不同无效 ID，默认模式仍会产生存在性查询。仅当商铺写入端已接入失效协议、业务可接受过滤器刷新窗口时，可配置 trust_bloom: true，让 Bloom 阴性直接返回。

## Redis 与多实例失效

| 键或频道 | 用途 |
|---|---|
| xzdp:&lt;env&gt;:shop:detail:&lt;id&gt; | 商铺详情 |
| xzdp:&lt;env&gt;:shop:types:all | 分类列表 |
| 业务键加 :fill | 4 秒互斥租约 |
| xzdp:&lt;env&gt;:shop:invalidate | Pub/Sub，消息体为待失效的完整缓存键 |

未来的商铺写入端应在 DB 提交后调用 ShopCache.Invalidate(ctx, id)，分类变更使用 id=0。它通过 Redis 事务删除缓存和租约并发布通知；返回错误时写入方需重试。实例在订阅或重连确认时清空 L1。每次失效递增本地版本，失效前已开始的查询不能把旧值重新填入 L1。直接在 MySQL 修改数据的操作可通过失效协议通知应用，否则依赖 TTL 刷新。

## 验收

在项目根目录运行：

```sh
export DOCKER_CONTEXT=colima-gcfeed
docker compose up -d --build --wait
go test ./...
go test -race -count=3 ./biz/pkg/cache ./biz/service/shop
go vet ./...
go build ./...
bash script/smoke-phase0.sh
python3 script/bench-shop-phase1.py
```

压测使用本机单 API 实例和种子商铺 2。脚本先填充缓存，令 L2 到期，广播失效并确认 API 已处理，然后建立 500 条连接并统一放行。每个响应都校验商铺 ID。脚本断言详情回源与额外存在性查询合计恰好一次，P99 从放行到读完整个响应计算。运行时应关闭其他商铺详情流量，以免其查询计入同一实例指标。可用 SHOP_ID 指定另一条存在的种子数据。

/metrics 提供 L1、L2 的 hits、misses 与 hit_ratio，详情和分类的 DB 加载次数、存在性探测次数、Bloom 拒绝数和失效通知数。L2 指标统计实际回填流程的 Redis 读取，singleflight 合并的等待者共享结果。

## 本机验收记录

2026-11-21，最终 API 镜像在 Colima 单实例运行。500 个请求全部读到目标商铺，详情与存在性 SQL 增量合计 1 次，客户端 P99 为 66.07 ms（单轮结果）。真实 Redis 停机时，携带 Authorization 的公开详情请求仍通过 MySQL 读到未预热的商铺 5 和 6。分类接口返回 10 个不同的分类 ID。

两个独立 L1、共享一个模拟 Redis 的测试中，500 个跨节点请求共执行一次详情查询。测试还覆盖失效与回填并发、订阅重连、Redis 无响应、单个等待者取消和新商铺出现在 Bloom 快照之后的情形。
