"""Exercise both feed modes through the public API against the default Compose stack."""

import json
import urllib.request

from lifecycle import poll
from sale import PORT, VOUCHER, compose, sessions, sql


def call(path, token, body=None, method=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}",
        data=json.dumps(body).encode() if body else None,
        method=method,
        headers={"Authorization": token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        result = json.load(response)
    assert result["success"], result
    return result["data"]


def run():
    normal, popular, viewer = sessions(3)
    a, b, reader = VOUCHER, VOUCHER + 1, VOUCHER + 2
    sql(f"INSERT INTO tb_follow(user_id,follow_user_id) VALUES ({reader},{a}),({reader},{b})")
    try:
        sql(f"""INSERT INTO tb_follow(user_id,follow_user_id)
             SELECT {reader}+ROW_NUMBER() OVER(),{b}
             FROM tb_user a CROSS JOIN tb_user b CROSS JOIN tb_user c LIMIT 9999""")
        body = {
            "shopId": 1,
            "title": "feed integration",
            "content": "test",
            "images": "fixture.jpg",
        }
        pushed = call("/blog", normal, body)["id"]
        pulled = call("/blog", popular, body)["id"]
        poll(lambda: sql(f"SELECT delivered::int FROM feed_publication WHERE blog_id={pushed}") == "1")
        assert sql(f"SELECT pull::int FROM feed_publication WHERE blog_id={pulled}") == "1"
        inbox = f"feed:{reader}"
        assert compose("redis", ["redis-cli", "--raw", "ZSCORE", inbox, str(pushed)])
        assert not compose("redis", ["redis-cli", "--raw", "ZSCORE", inbox, str(pulled)])
        # Ten stale, newer entries must not truncate the first visible page.
        stale = [str(VOUCHER + 100 + i) for i in range(10)]
        future = 2100000000
        args = [v for item in stale for v in (str(future), item)]
        compose("redis", ["redis-cli", "ZADD", inbox, *args])
        items = call("/blog/of/follow?lastId=2100000000&offset=0", viewer)["list"]
        assert {item["id"] for item in items} == {pushed, pulled}, items
        call(f"/blog/{pushed}", normal, method="DELETE")
        remaining = call("/blog/of/follow?offset=0", viewer)["list"]
        assert [item["id"] for item in remaining] == [pulled], remaining
        print("PASS regular post pushed, popular post pulled, both visible once in timeline")
    finally:
        sql(f"""DELETE FROM feed_publication WHERE author_id IN ({a},{b});
                DELETE FROM tb_blog WHERE user_id IN ({a},{b});
                DELETE FROM tb_follow WHERE follow_user_id IN ({a},{b})""")


if __name__ == "__main__":
    run()
