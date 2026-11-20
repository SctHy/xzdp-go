# 附近商铺

读接口沿用 GET /shop/of/type/geo，坐标参数 x、y，dist 及返回距离采用米。客户端读 Redis 地理索引，再通过一次按主键批量查询补齐当页详情。

迁移 006_nearby.sql 创建 shop_geo_dirty 表和三个 MySQL 触发器。新增、删除或更新位置和分类会在同一事务中登记商铺 ID。Go 任务启动时装载已有商铺，以后一秒轮询一次待更新的 ID，每批最多 64 个。Redis 写入成功后才提交移除待更新记录。Lua 同时处理旧分类移除和新坐标写入，重复处理同一个 ID 会收敛到当前坐标。同步期间有短暂可见性延迟。键使用 xzdp:<env>:geo:<分类> 和同前缀的 categories、ready。

Compose 的本地 MySQL 为创建触发器打开 log_bin_trust_function_creators。其他数据库部署可由具备触发器权限的迁移账户预先执行 DDL。执行 go test ./biz/service/shop -run TestNearbyReadsOnlyMatchedIDs 检查十次查询均只按命中的 ID 取详情；python3 script/load/nearby.py 在 Compose 上验证新增、移动、换类和删除。
