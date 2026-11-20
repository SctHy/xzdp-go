# Phase 0 基座审计与运行

审计起点为 `lhpqaq/xzdp-go@8be36c8`。服务使用 Hertz、GORM、MySQL 和 Redis。入口为 `main.go`，自定义路由在 `router.go`；`idl/` 定义业务 API，`biz/{router,handler,service,dal}` 分别负责路由、请求、业务和存储。

## 本地运行

```sh
docker compose up -d --build --wait
bash script/smoke-phase0.sh
go test ./...
go build ./...
docker compose down
```

冒烟脚本使用 Bash、curl 和 Python 3，创建本地测试用户和临时 Kafka topic，并短暂停止本项目的 Redis 验证探针。MySQL 空卷首次导入 `resources/xzdp.sql`；普通重启保留数据。该种子 SQL 含 DROP TABLE，仅供新库初始化。本阶段没有 SQL migration。

| 服务 | 容器内存上限 | 本地访问 |
|---|---:|---|
| API | 192 MiB | 127.0.0.1:8081 |
| MySQL 8 | 512 MiB | Compose 网络 mysql:3306 |
| Redis 7 | 128 MiB | redis:6379 |
| Kafka KRaft | 768 MiB | kafka:9092 |
| Prometheus | 256 MiB | 127.0.0.1:9090 |
| Grafana | 256 MiB | 127.0.0.1:3000 |

容器上限合计 2112 MiB，另需 Docker 虚拟机及构建余量。Kafka 为单节点，堆上限 256 MiB。Prometheus 采集 API 的 `xzdp_build_info`。Grafana 数据源 UID 是 `xzdp-prometheus`。本地默认 Grafana 用户和密码均为 admin；端口可用 API_PORT、PROMETHEUS_PORT、GRAFANA_PORT 调整。

## 配置与探针

运行时优先级为环境变量、YAML、内建默认值。通过 XZDP_CONFIG 指定 YAML，或使用 conf/$GO_ENV/conf.yaml；默认 GO_ENV=test。可覆盖 XZDP_HTTP_ADDRESS、XZDP_LOG_LEVEL、XZDP_MYSQL_DSN、XZDP_REDIS_ADDRESS、XZDP_REDIS_PASSWORD、XZDP_PPROF。pprof 默认关闭，启用后只监听进程所在网络命名空间的 127.0.0.1:6060。

GET /health/live 返回进程状态。GET /health/ready 在一秒预算内并行检查 MySQL 和 Redis，进入排空或依赖异常时返回 503。SIGTERM/SIGINT 会结束 SSE 并在十秒内排空 HTTP 请求，再释放依赖。业务响应兼容原前端；统一错误包含 success、errorMsg、code 和 requestId，响应头带 X-Request-ID。

## 数据结构

| 域 | 表与约束 |
|---|---|
| 用户 | tb_user（id 主键、phone 唯一）、tb_user_info（user_id 主键）、tb_sign |
| 商铺 | tb_shop、tb_shop_type，均为 id 主键 |
| 内容 | tb_blog、tb_blog_comments、tb_follow，均为 id 主键 |
| 优惠券 | tb_voucher（id 主键）、tb_seckill_voucher（voucher_id 主键）、tb_voucher_order（id 主键） |

现有 Key 包括 login:code:&lt;phone&gt;、login:token:&lt;token&gt;、cache:shop:&lt;id&gt;。本阶段没有新增业务 Key。原订单实现与随仓表的 order_id/id 差异以及用户与券的唯一约束，归入 Phase 2。

## 已注册 API

- `GET /blog/hot`
- `GET /blog/user/:id`
- `GET /blog/of/me`
- `POST /blog`
- `GET /blog/:id`
- `DELETE /blog/:id`
- `PUT /blog/like/:id`
- `GET /blog/likes/:id`
- `GET /blog/of/follow`
- `GET /comment/hot`
- `GET /comment/:blogID`
- `POST /comment/post`
- `PUT /comment/like/:id`
- `DELETE /comment/:id`
- `PUT /follow/:id/:isFollow`
- `GET /follow/or/not/:id`
- `GET /follow/common/:id`
- `POST /upload/blog`
- `GET /message/sse`
- `GET /shop-type/list`
- `GET /shop/of/type`
- `GET /shop/of/type/geo`
- `GET /shop/:id`
- `GET /user/me`
- `POST /user/code`
- `POST /user/login`
- `GET /user/info/:id`
- `POST /user/sign`
- `GET /user/sign/count`
- `GET /voucher/list/:id`
- `POST /voucher-order/seckill/:id`
- `GET /hello`

自定义入口还包括 /ping、/imgs/*filepath、/health/live、/health/ready、/metrics。

## 本次验证记录

2026-11-21 在 Apple Silicon / Colima 的独立 Compose 项目 xzdp 上完成。宿主机 Go 1.27.3 全量测试、构建和 vet 通过，Linux API 镜像使用 Go 1.22.12 构建。健康、鉴权和配置包通过竞态检测。

真实容器验证通过验证码登录、当前用户、商铺详情、热门笔记、未登录抢券拒绝、Kafka 单条消息收发、Prometheus 抓取和 Grafana 数据源查询。停止 Redis 后 live=200、ready=503，恢复后 ready=200。向 API 发送 SIGTERM 后容器退出码为 0，重启恢复就绪。

本机使用已有 Docker context。复验前在终端执行 `export DOCKER_CONTEXT=colima-gcfeed`。这只影响当前终端。本项目的六个容器可用 `docker compose stop` 停止以释放内存。
