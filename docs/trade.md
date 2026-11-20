# 订单契约

HTTP 成功结果在 data 字段，订单号以十进制字符串传输，避免 JavaScript 精度损失。所有接口均使用既有 Authorization 登录凭据。POST /voucher-order/token/:id 获取本人当前活动的 30 秒令牌；POST /voucher-order/seckill/:id 通过 X-Seckill-Token 提交；GET /voucher-order/status/:id 查询本人订单。领令牌和提交采用 Redis 滑动窗口，默认每秒每 IP 2500、每路径 5000、每用户 10。重复领令牌在有效期内返回同一凭据。

```mermaid
sequenceDiagram
 participant H as Hertz
 participant R as Redis
 participant K as Kafka
 participant D as MySQL
 H->>R: Lua 验令牌、扣库存、记录买家与 pending
 R-->>H: 排队订单号
 R->>K: relay 批量发送
 K->>D: 消费凭据、订单、扣库存同事务提交
 D-->>R: 清理 pending
```

internal/trade/sale.go 管理资格，worker.go 负责传输和落库，http.go 只处理协议。internal/migrate/sql 中的前向 SQL 同时适用于新卷和旧卷。迁移锁串行执行 DDL；历史重复订单会阻止唯一索引创建，运维人员应先核查历史业务数据。消费失败重试三次后写 Kafka DLQ 与 MySQL 死信表。Redis pending 的生命周期独立于订单业务状态。Redis 必须持久化且禁止驱逐交易键。

Redis 前缀为 xzdp:<env>:trade:。v:<id> 保存库存与时间窗口，:buyers 是用户到订单的映射，:token:<user> 是短期凭据；pending 保存未完成的投递记录，relay 按下次投递时间排序。序列键 sequence 不得重置。Kafka 主 topic 为 xzdp-<env>-orders，死信 topic 加 .dlq。

设计对照：[Go Lua 入队](https://github.com/wengyuechuan/hm-dianping-go/blob/main/script/seckill.lua)、[Go Kafka pending 重投](https://github.com/amemiya02/hmdp-go/blob/main/internal/service/voucher_order.go)。本项目使用同一个订单号串起预扣、投递和 DB 凭据。

## 关单、候补与提醒

POST /voucher-order/cancel/:id 取消本人未支付订单；POST /voucher-order/wait/:id 对售罄的活动候补；POST /voucher-order/subscribe/:id 订阅未开始活动；GET /notifications 返回本人最近 50 条通知。订单状态只在 SQL 中使用条件更新。定时任务扫描超过 15 分钟的未支付订单，复用取消事务。
数据库迁移 003 增加库存释放事件、订阅与站内通知。释放任务在 Redis 利用订单号去重，将库存交给最早的候补者或退回库存。候补订单复用同一条 Kafka 消费链。订阅提醒在开抢前五分钟写入通知表，再发送到已有 SSE Stream。
参考 [Java Plus 延时通知](https://github.com/java-up-up/hmdp-plus/tree/HEAD/hmdp-core-service/src/main/java/org/javaup/delay/consumer) 和 [go-zero 订单生命周期](https://github.com/me-cs/dianping-gozero/tree/main/rpc/order/internal/logic)。
