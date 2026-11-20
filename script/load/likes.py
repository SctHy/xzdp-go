"""Fixed-rate unique likes on a dedicated blog. Compare SQL UPDATE commands per test second."""

import asyncio
import json
import time
from sale import VOUCHER, request, sessions, sql


async def run():
    blog = VOUCHER
    sql(
        f"INSERT INTO tb_blog(id,shop_id,user_id,title,images,content) VALUES({blog},1,1,'load','','load')"
    )
    users = sessions(1000)

    def updates():
        return int(sql("SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname='tb_blog'"))

    before = updates()
    start = time.monotonic()
    tasks = []
    for i in range(20):
        await asyncio.sleep(max(0, start + i * 0.25 - time.monotonic()))
        tasks.extend(
            asyncio.create_task(request(f"/blog/like/{blog}", u, method="PUT"))
            for u in users[i * 50 : (i + 1) * 50]
        )
    result = await asyncio.gather(*tasks)
    assert all(r["success"] and r["data"]["isLiked"] for r, _ in result), result[0]
    end = time.monotonic() + 10
    while sql(f"SELECT liked FROM tb_blog WHERE id={blog}") != "1000":
        assert time.monotonic() < end, "count did not converge"
        await asyncio.sleep(0.1)
    elapsed = time.monotonic() - start
    count = updates() - before
    print(
        json.dumps(
            dict(
                requests=1000,
                likes=1000,
                sql_updates=count,
                seconds=round(elapsed, 3),
                update_qps=round(count / elapsed, 2),
            )
        )
    )


if __name__ == "__main__":
    asyncio.run(run())
