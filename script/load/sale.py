"""Local Compose acceptance: create an isolated sale, release 1000 requests, inspect SQL."""

import asyncio
import json
import os
import secrets
import subprocess
import time

PORT = int(os.getenv("API_PORT", "8081"))
VOUCHER = time.time_ns() // 1000
USERS = 1000
STOCK = 100


def compose(service, args, data=None):
    return subprocess.check_output(
        ["docker", "compose", "exec", "-T", service, *args], input=data, text=True
    ).strip()


def sql(text):
    return compose("mysql", ["sh", "-c", 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -uxzdp xzdp'], text)


def sessions(users):
    tokens = [secrets.token_hex(16) for _ in range(users)]
    records = []
    for i, token in enumerate(tokens):
        records += [
            f"HSET login:token:{token} id {VOUCHER+i} nick_name load",
            f"EXPIRE login:token:{token} 600",
        ]
    compose("redis", ["redis-cli"], "\n".join(records))
    return tokens


async def request(path, auth, sale_token="", gate=None, method="POST"):
    for attempt in range(6):
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
            break
        except OSError:
            if attempt == 5:
                raise
            await asyncio.sleep(0.1 * (attempt + 1))
    try:
        if gate:
            await gate()
        start = time.perf_counter()
        writer.write(
            (
                f"{method} {path} HTTP/1.0\r\nHost: localhost\r\nAuthorization: {auth}\r\n"
                f"X-Seckill-Token: {sale_token}\r\nContent-Length: 0\r\n\r\n"
            ).encode()
        )
        await writer.drain()
        head, body = (await asyncio.wait_for(reader.read(), 20)).split(b"\r\n\r\n", 1)
        assert head.split()[1] == b"200", head
        return json.loads(body), (time.perf_counter() - start) * 1000
    finally:
        writer.close()
        await writer.wait_closed()


async def run():
    sql(
        f"INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES({VOUCHER},{STOCK},NOW()-INTERVAL 1 MINUTE,NOW()+INTERVAL 1 HOUR)"
    )
    users = sessions(USERS)
    sem = asyncio.Semaphore(50)

    async def ticket(user):
        async with sem:
            data, _ = await request(f"/voucher-order/token/{VOUCHER}", user)
            assert data["success"], data
            return data["data"]

    tokens = await asyncio.gather(*(ticket(u) for u in users))
    ready = asyncio.Event()
    count = 0

    async def gate():
        nonlocal count
        count += 1
        if count == USERS:
            ready.set()
        await asyncio.wait_for(ready.wait(), 15)

    tasks = []
    for i, (u, t) in enumerate(zip(users, tokens)):
        tasks.append(asyncio.create_task(request(f"/voucher-order/seckill/{VOUCHER}", u, t, gate)))
        if i % 25 == 0:
            await asyncio.sleep(0.02)
    results = await asyncio.gather(*tasks)
    success = [r for r, _ in results if r["success"]]
    assert len(success) == STOCK, len(success)
    assert all(r["success"] or r["errorMsg"] == "已售罄" for r, _ in results)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        totals = sql(
            f"SELECT COUNT(*),COUNT(DISTINCT user_id),(SELECT stock FROM tb_seckill_voucher WHERE voucher_id={VOUCHER}) FROM tb_voucher_order WHERE voucher_id={VOUCHER}"
        )
        if totals == f"{STOCK}\t{STOCK}\t0":
            break
        await asyncio.sleep(0.25)
    else:
        raise AssertionError(totals)
    ordered = sorted(ms for _, ms in results)
    print(
        json.dumps(
            dict(
                voucher=VOUCHER,
                requests=USERS,
                accepted=STOCK,
                orders=STOCK,
                stock=0,
                p99_ms=round(ordered[989], 2),
            )
        )
    )


if __name__ == "__main__":
    asyncio.run(run())
