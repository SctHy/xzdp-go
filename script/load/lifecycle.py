"""Black-box integration against the local Compose stack; only the fixture clock is advanced."""
import asyncio
import json
import time
import urllib.request
from sale import PORT, VOUCHER, request, sessions, sql, compose


def poll(fn):
    end=time.monotonic()+20
    while time.monotonic()<end:
        value=fn()
        if value:
            return value
        time.sleep(.15)
    raise RuntimeError("timed out waiting for fixture")


def inbox(token):
    req=urllib.request.Request(f"http://127.0.0.1:{PORT}/notifications",headers={"Authorization":token})
    return json.load(urllib.request.urlopen(req,timeout=3))["data"]


async def run():
    u, waiter=sessions(2)
    v=VOUCHER
    sql(f"INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES({v},1,NOW()+INTERVAL 3 MINUTE,NOW()+INTERVAL 1 HOUR)")
    sub,_=await request(f"/voucher-order/subscribe/{v}",waiter)
    assert sub["success"],sub
    poll(lambda:any("开抢" in n["content"] for n in inbox(waiter)))
    sql(f"UPDATE tb_seckill_voucher SET begin_time=NOW()-INTERVAL 1 SECOND WHERE voucher_id={v}")
    tok,_=await request(f"/voucher-order/token/{v}",u)
    accepted,_=await request(f"/voucher-order/seckill/{v}",u,tok["data"])
    original=int(accepted["data"])
    poll(lambda:sql(f"SELECT status FROM tb_voucher_order WHERE id={original}")=="1")
    joined,_=await request(f"/voucher-order/wait/{v}",waiter)
    assert joined["success"],joined
    sql(f"UPDATE tb_voucher_order SET create_time=NOW()-INTERVAL 16 MINUTE WHERE id={original}")
    def promoted():
        return sql(f"SELECT id FROM tb_voucher_order WHERE user_id={v+1} AND voucher_id={v} AND status=1")
    new=poll(promoted)
    poll(lambda:any(new in n["content"] for n in inbox(waiter)))
    assert sql(f"SELECT status FROM tb_voucher_order WHERE id={original}")=="4"
    assert sql(f"SELECT stock FROM tb_seckill_voucher WHERE voucher_id={v}")=="0"
    poll(lambda: int(compose("redis",["redis-cli","XLEN",f"message.stream:{v+1}"]))>=2)
    print("PASS subscription, sellout, FIFO wait, timeout, promotion, durable inbox and SSE stream")


if __name__=="__main__":
    asyncio.run(run())
