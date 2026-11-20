"""Run against local Compose with XZDP_MOCK_PAYMENT=true."""

import asyncio
import json
import urllib.request

from lifecycle import poll
from sale import PORT, VOUCHER, request, sessions, sql


def redeem(order, token, code):
    req = urllib.request.Request(
        f"http://localhost:{PORT}/voucher-order/redeem/{order}",
        data=json.dumps({"code": code}).encode(),
        headers={"Content-Type": "application/json", "Authorization": token},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


async def run():
    buyer, clerk, outsider, other, late = sessions(5)
    v = VOUCHER
    sql(
        f"""INSERT INTO tb_voucher(id,shop_id,title,pay_value,actual_value) VALUES({v},{v},'checkout fixture',1,1);
    INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES({v},3,NOW()-INTERVAL 1 MINUTE,NOW()+INTERVAL 1 HOUR);
    INSERT INTO shop_staff VALUES({v},{v+1}),({v+1},{v+2});"""
    )

    async def act(action, order, user):
        data, _ = await request(f"/voucher-order/{action}/{order}", user)
        return data

    async def order(user):
        token = await act("token", v, user)
        result, _ = await request(f"/voucher-order/seckill/{v}", user, token["data"])
        oid = int(result["data"])
        poll(lambda: sql(f"SELECT status FROM tb_voucher_order WHERE id={oid}") == "1")
        return oid

    oid = await order(buyer)
    assert not (await act("pay", oid, outsider))["success"]
    payments = await asyncio.gather(act("pay", oid, buyer), act("pay", oid, buyer))
    codes = {r["data"]["code"] for r in payments}
    assert len(codes) == 1
    code = codes.pop()
    assert not (await act("cancel", oid, buyer))["success"]
    assert not redeem(oid, outsider, code)["success"]
    assert not redeem(oid, buyer, code)["success"]
    assert not redeem(oid, clerk, "0" * 32)["success"]
    results = await asyncio.gather(
        *(asyncio.to_thread(redeem, oid, clerk, code) for _ in range(2))
    )
    assert sum(r["success"] for r in results) == 1
    status, _ = await request(f"/voucher-order/status/{oid}", buyer, method="GET")
    assert status["data"] == "used"
    assert sql(
        f"SELECT redeemed_by FROM voucher_redemption WHERE order_id={oid}"
    ) == str(v + 1)
    race = await order(other)
    payment, cancel = await asyncio.gather(
        act("pay", race, other), act("cancel", race, other)
    )
    assert payment["success"] != cancel["success"]
    assert sql(f"SELECT status FROM tb_voucher_order WHERE id={race}") == (
        "2" if payment["success"] else "4"
    )
    expired = await order(late)
    # Hold the order lock across the deadline. The payment starts before expiry
    # and must use database time after it obtains that lock.
    lock = f"checkout-test-{v}"

    async def hold():
        await asyncio.to_thread(
            sql,
            f"""START TRANSACTION;
            UPDATE tb_voucher_order SET create_time=NOW()-INTERVAL 15 MINUTE+INTERVAL 3 SECOND WHERE id={expired};
            SELECT GET_LOCK('{lock}',0); SELECT SLEEP(4); COMMIT;""",
        )

    holder = asyncio.create_task(hold())
    await asyncio.to_thread(
        poll, lambda: sql(f"SELECT IS_USED_LOCK('{lock}') IS NOT NULL") == "1"
    )
    assert not (await act("pay", expired, late))["success"]
    await holder
    poll(lambda: sql(f"SELECT status FROM tb_voucher_order WHERE id={expired}") == "4")
    assert (
        sql(f"SELECT COUNT(*) FROM voucher_redemption WHERE order_id={expired}") == "0"
    )
    sql(
        f"DELETE FROM shop_staff WHERE shop_id IN ({v},{v+1}) AND user_id IN ({v+1},{v+2})"
    )
    print(
        "PASS buyer ownership, concurrent pay retry, staff scope, single redemption, pay/cancel race and expiry"
    )


if __name__ == "__main__":
    asyncio.run(run())
