"""Inject a stranded reservation on a fresh local fixture, then observe automatic repair."""

import asyncio
import os
from lifecycle import poll
from sale import VOUCHER, compose, request, sessions, sql


async def run():
    voucher = VOUCHER
    order = VOUCHER + (1 << 60)
    user = sessions(1)[0]
    sql(f"""INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time)
        VALUES({voucher},1,NOW()-INTERVAL '1 minute',NOW()+INTERVAL '1 hour')""")
    token, _ = await request(f"/voucher-order/token/{voucher}", user)
    assert token["success"], token
    prefix = "xzdp:" + os.getenv("GO_ENV", "test") + ":trade:"
    sql(f"""INSERT INTO trade_dead_letter(order_id,voucher_id,user_id,reason)
            VALUES({order},{voucher},{voucher},'integration fixture')""")
    compose(
        "redis",
        ["redis-cli"],
        f"""MULTI
HINCRBY {prefix}v:{voucher} stock -1
HSET {prefix}v:{voucher}:buyers {voucher} {order}
HSET {prefix}pending {order} {voucher}:{voucher}
EXEC
""",
    )

    def settled():
        return (
            sql(
                f"""SELECT COUNT(*),(SELECT stock FROM tb_seckill_voucher WHERE voucher_id={voucher}),
                       (SELECT COUNT(*) FROM trade_receipt WHERE order_id={order})
                       FROM tb_voucher_order WHERE id={order} AND user_id={voucher}"""
            )
            == "1\t0\t1"
        )

    poll(settled)
    poll(lambda: compose("redis", ["redis-cli", "HEXISTS", prefix + "pending", str(order)]) == "0")
    status, _ = await request(f"/voucher-order/status/{order}", user, method="GET")
    assert status["data"] == "unpaid", status
    print("PASS stranded Redis reservation and DLQ entry recovered to one SQL order and receipt")


if __name__ == "__main__":
    asyncio.run(run())
