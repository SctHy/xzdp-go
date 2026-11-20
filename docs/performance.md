# 架构演进与压测

## 从入口到持久化

项目保留 Hertz 单体。商铺读取、交易和社交共用连接池，各自管理数据与后台任务。订单的业务状态存于 MySQL；Redis 的排队记录、Kafka 的消费位点、站内信的投递标记分别回答「已受理吗」「已消费吗」「已通知吗」。消费者重放不会让订单再次扣库存。[基座审计](phase-0-audit.md) · [缓存契约](phase-1-cache.md) · [订单与社交契约](trade.md)

| 功能 | 决策与代价 |
|---|---|
| 热点商铺 | FreeCache → Redis → MySQL。singleflight 合并同节点回源；Redis 租约约束跨节点回填，失效通知驱逐 L1。短 TTL 限制广播丢失后的陈旧窗口。 |
| 抢券 | Lua 校验限流、短时令牌和库存；预扣与待投递记录同一脚本提交。成功响应只承诺受理，客户端用订单号查询结果。Redis AOF、MySQL 事务与消费幂等分别覆盖不同故障点。 |
| 关单与候补 | 未支付满 15 分钟，条件事务关单、回库存并写释放事件。释放 worker 将名额交给最早候补者。提醒由独立任务写站内信，再复用 SSE。 |
| 点赞与 Feed | Redis 先承接点赞，四个 worker 合并同一笔记的更新。每次发帖在事务中记录推送或拉取模式。读时合并、去重，粉丝数跨阈值后旧帖仍可见。 |

## 本地验收

2026-11-21，Apple Silicon、16 GiB 宿主机，Colima Linux ARM64 分配 4 vCPU、约 5.8 GiB。单 API 容器上限 192 MiB，Go 1.22.12，单节点 Kafka、MySQL、Redis。保留结构化访问日志。下表是这一台机器的测量值。

| 场景 | 对照 | 最终链路 | 校验方式 |
|---|---:|---:|---|
| 500 连接同时读过期商铺 | Phase 1 单轮 P99 66.07 ms | 20.24、25.45 ms | 500 条响应均为目标商铺，详情 SQL 与存在性探测合计 1 次 |
| 持续读热商铺，50 VU，15 秒 | 27,657 req/s，P99 4.92 ms | 33,428 req/s，P99 3.52 ms | k6 逐条验证商铺 ID，分别完成 414,892 与 501,456 次请求，零错误 |
| 1,000 用户同时抢 100 张券 | 三轮 P99 为 59.05、83.64、65.23 ms | 48.75、49.97、50.48 ms | 每轮 100 单、100 个不同买家、SQL 剩余 0。中位 P99 降低 23.4% |
| 1,000 次点赞，每 250 ms 发 50 次 | 1,000 次 SQL UPDATE，205.03 次/s | 6 次 UPDATE，1.15 次/s | 计时到 MySQL 收敛为 1,000；UPDATE QPS 下降 99.4% |

商铺持续读和秒杀对照使用提交 `a28418c`。它已有多级缓存、Redis 预扣与 Kafka 消费。上游原版订单列名与种子表不一致，无法取得成功下单的同口径 P99。并发秒杀延迟从统一放行算到收到受理或售罄响应，覆盖全部 1,000 个请求；随后独立轮询 MySQL 至库存和唯一买家数达标。点赞基线为 `0445cdf` 的逐请求写库实现，脚本读取专用测试栈的 MySQL 全局 `Com_update`，并等待最终一致。复验点赞时记录到 5 次 UPDATE、0.99 次/s，最终计数仍是 1,000。后台任务和本机调度均会影响单轮数值。

## CPU 火焰图

在两次 50 VU 压测的前 10 秒采集 pprof。优化前，`encoding/json.Unmarshal` 累计占 16.2% 的 CPU 样本，调用来自 L1 命中后的商铺详情解码。HTTP 路径现在直接将共享缓存的 JSON 放进原响应结构，内部需要对象的调用继续使用类型化接口。相同路由下，详情缓存函数的累计占比由约 19.0% 降到 3.3%。日志仍约占优化后 CPU 的 13.9%，本次对照两端均开启访问日志。[优化前火焰图](performance/hot-shop-before.svg) · [优化后火焰图](performance/hot-shop-after.svg) · [优化前 pprof](performance/hot-shop-before.pprof) · [优化后 pprof](performance/hot-shop-after.pprof)

秒杀入口还合并了会话读取与续期的 Redis 往返，将访问人数的 HyperLogLog 更新移到独立批处理任务，订单号在预扣脚本确认资格后生成。Redis 的整数发号结果按字符串传输，避免 Lua 或浏览器将高位整数舍入。

## 复现

需要 Docker Compose、Python 3、k6。先在仓库根目录启动服务。下列验收脚本会向本地库写入独立测试数据，冒烟脚本会短暂停止当前 Compose 项目的 Redis 与 API。

```sh
docker compose up -d --build --wait
go test ./...
go test -race ./internal/... ./biz/pkg/cache ./biz/service/shop
go vet ./... && go build ./...
bash script/smoke-phase0.sh
python3 script/bench-shop-phase1.py
python3 script/load/sale.py
python3 script/load/reconcile.py
python3 script/load/lifecycle.py
python3 script/load/likes.py
python3 script/load/feed.py
k6 run script/load/hot-shop.js
```

秒杀 k6 为每位用户分配一个 VU。setup 先领令牌，随后每个 VU 仅提交一次；脚本分别核对响应、受理总数与 SQL。夹具使用 0600 权限临时文件保存本地测试会话。k6 在领令牌后错峰建立连接并统一放行，避免 macOS 端口转发在同一瞬间建立 1,000 条新连接导致 TCP reset。两轮 k6 独立验收的下单 P99 为 66.78、63.83 ms，1,000 个业务断言全部通过，SQL 核对 100 单和 100 个不同买家。VM 内运行 Python 并发脚本前可用 `ulimit -n 4096` 提高当前 shell 的描述符上限。[k6 执行器说明](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/per-vu-iterations/)

```sh
fixture=$(mktemp)
python3 script/load/sale-fixture.py prepare "$fixture"
SALE_FIXTURE="$fixture" k6 run script/load/seckill.js
python3 script/load/sale-fixture.py verify "$fixture"
rm "$fixture"
```

## 指标与剖析

Prometheus 每 10 秒抓取 `/metrics`。Grafana 已配置对应数据源，可在 Explore 中运行以下查询。HTTP 的 route 标签使用注册模板。同一路由的业务售罄仍返回兼容前端的 HTTP 200，由 Lua 拒绝计数反映。

| 问题 | PromQL |
|---|---|
| 各路由每秒多少请求？ | `sum by (route) (rate(xzdp_http_requests_total[1m]))` |
| 各路由 P99 是多少秒？ | `histogram_quantile(0.99, sum by (le,route) (rate(xzdp_http_request_duration_seconds_bucket[5m])))` |
| 各缓存层最近命中率？ | `sum by (level) (rate(xzdp_shop_cache_hits_total[5m])) / (sum by (level) (rate(xzdp_shop_cache_hits_total[5m])) + sum by (level) (rate(xzdp_shop_cache_misses_total[5m])))` |
| 消费者落后多少条？ | `xzdp_kafka_consumer_lag`，订单 topic 各分区的末尾位点减去消费组提交位点。采样时间见 `xzdp_kafka_lag_last_success_timestamp_seconds`，首次成功前 lag 为 -1。 |
| 每秒多少次 Lua 拒绝？ | `rate(xzdp_lua_rejections_total[1m])` |

pprof 默认关闭。按以下命令在容器内回环接口采样，另一终端同时运行 k6。报告火焰图使用 pprof 的带权样本栈和 FlameGraph 渲染。[Go pprof](https://go.dev/blog/pprof) · [FlameGraph](https://github.com/brendangregg/FlameGraph)

```sh
XZDP_PPROF=true docker compose up -d --wait api
docker compose exec -T api wget -qO- 'http://127.0.0.1:6060/debug/pprof/profile?seconds=10' > /tmp/xzdp.cpu
go tool pprof -top -cum /tmp/xzdp.cpu
docker compose up -d --wait api
```

## 简历与面试

可写为「基于 Hertz、Redis、Kafka 和 MySQL 实现本地生活点评后端；在单实例 1,000 并发抢 100 张券测试中，数据库核验 100 个不同买家、零超卖和零少卖；热点点赞的 MySQL UPDATE QPS 降低 99.4%。」

面试时可以沿一笔订单解释三个成功时点：Redis 已受理、MySQL 已提交、站内信已投递。演示 `reconcile.py` 如何注入「已预扣、死信存在、订单缺失」并自动补单；用 `lifecycle.py` 展示关单后候补者收到通知。单机 Compose 的结果覆盖功能和本地负载，跨可用区故障切换、持续峰值容量和备份恢复仍需在部署环境演练。
