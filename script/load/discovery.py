"""HTTP acceptance against the local Compose stack and Mailpit."""

import concurrent.futures
import datetime
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from sale import PORT, VOUCHER, sessions, sql, compose


def http(path, user="", body=None, key=None):
    headers = {"Authorization": user, "Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(
        "http://localhost:" + str(PORT) + path,
        headers=headers,
        data=json.dumps(body).encode() if body is not None else None,
    )
    with urllib.request.urlopen(req, timeout=8) as res:
        return json.load(res)


def together(fn, values):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(values)) as pool:
        return list(pool.map(fn, values))


def search_booking(users):
    v = VOUCHER
    types = 9800000 + v % 100000
    shops = [
        f"({v+i},'{('needle' if i==11 else 'closer')}',{types},'','fixture',{120+i*.0001},30,0,0,0)"
        for i in range(12)
    ]
    sql(
        "INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score) VALUES"
        + ",".join(shops)
    )
    sql(f"INSERT INTO shop_staff VALUES({v},{v})")
    try:
        result = http(
            f"/shops/search?typeId={types}&x=120&y=30&radiusMeters=1000&q=needle"
        )
        assert [x["id"] for x in result["data"]] == [v + 11], result
        now = datetime.datetime.now(datetime.timezone.utc)
        slot = {
            "startsAt": (now + datetime.timedelta(hours=1)).isoformat(),
            "endsAt": (now + datetime.timedelta(hours=2)).isoformat(),
            "capacity": 1,
        }
        assert not http(f"/shops/{v}/slots", users[1], slot)["success"]
        sid = http(f"/shops/{v}/slots", users[0], slot)["data"]
        assert http(f"/shops/{v}/slots")["data"][0]["remaining"] == 1
        path = f"/slots/{sid}/bookings"
        book = lambda i: http(path, users[i], {"partySize": 1}, f"seat-{i}")
        results = together(book, list(range(1, 9)))
        winners = [i for i, r in enumerate(results, 1) if r["success"]]
        assert len(winners) == 1, results
        i = winners[0]
        bid = results[i - 1]["data"]["id"]
        full = together(
            lambda _: http(path, users[0], {"partySize": 1}, "no-capacity"),
            list(range(8)),
        )
        assert all(not r["success"] for r in full), full
        assert book(i)["data"]["id"] == bid
        assert not http(path, users[i], {"partySize": 2}, f"seat-{i}")["success"]
        assert not http(f"/bookings/{bid}", users[0])["success"]
        assert not http(f"/bookings/{bid}/cancel", users[0], {})["success"]
        assert all(
            r["success"]
            for r in together(
                lambda _: http(f"/bookings/{bid}/cancel", users[i], {}), [0, 1]
            )
        )
        assert http(f"/shops/{v}/slots")["data"][0]["remaining"] == 1
        repeats = together(
            lambda _: http(path, users[i], {"partySize": 1}, "late-cancel"),
            list(range(8)),
        )
        assert all(r["success"] for r in repeats), repeats
        ids = {r["data"]["id"] for r in repeats}
        assert len(ids) == 1 and http(f"/shops/{v}/slots")["data"][0]["remaining"] == 0
        different_key = ids.pop()
        sql(
            f"UPDATE booking_slot SET starts_at=UTC_TIMESTAMP()-INTERVAL 5 MINUTE WHERE id={sid}"
        )
        assert not http(f"/bookings/{different_key}/cancel", users[i], {})["success"]
    finally:
        sql(
            f"DELETE b FROM booking b JOIN booking_slot s ON s.id=b.slot_id WHERE s.shop_id={v};DELETE FROM booking_slot WHERE shop_id={v};DELETE FROM shop_staff WHERE shop_id={v};DELETE FROM tb_shop WHERE id BETWEEN {v} AND {v+11}"
        )
    print(
        "PASS search filters before paging and concurrent booking, replay, ownership and cancel"
    )


def reward(user):
    progress = http("/user/sign/reward", user)["data"]
    month = progress["month"]

    key = f"user:sign:{VOUCHER}:" + month[:7].replace("-", "")
    try:
        assert not http("/user/sign/reward/claim", user, {})["success"]

        required = http("/user/sign/reward", user)["data"]["daysRequired"]
        for i in range(required):
            compose("redis", ["redis-cli", "SETBIT", key, str(i), "1"])
        if datetime.datetime.now(datetime.timezone.utc).day >= required:
            results = together(
                lambda _: http("/user/sign/reward/claim", user, {}), list(range(8))
            )
            assert all(r["success"] and r["data"]["claimed"] for r in results), results
            assert (
                sql(f"SELECT COUNT(*) FROM sign_reward_grant WHERE user_id={VOUCHER}")
                == "1"
            )
        print("PASS monthly sign reward")
    finally:
        sql(f"DELETE FROM sign_reward_grant WHERE user_id={VOUCHER}")

        compose("redis", ["redis-cli", "DEL", key])


def email(users):
    inbox = "http://localhost:" + os.getenv("MAILPIT_PORT", "8025")

    def send(address, purpose, user=""):
        result = http("/user/email/code", user, {"email": address, "purpose": purpose})
        assert result["success"], result
        query = urllib.parse.urlencode({"query": "to:" + address})
        until = time.monotonic() + 8
        while time.monotonic() < until:
            msgs = json.load(urllib.request.urlopen(inbox + "/api/v1/search?" + query))[
                "messages"
            ]
            if msgs:
                msg = json.load(
                    urllib.request.urlopen(inbox + "/api/v1/message/" + msgs[0]["ID"])
                )
                return re.search(r"code is (\d{6})", msg["Text"])[1]
            time.sleep(0.1)
        raise AssertionError("message missing")

    addr = f"test-{VOUCHER}@example.test"
    code = send(addr, "login")
    body = {"email": addr, "code": code}
    assert not http("/user/email/code", "", {"email": addr, "purpose": "login"})[
        "success"
    ]
    results = together(lambda _: http("/user/email/login", "", body), [0, 1])
    tokens = [r["data"] for r in results if r["success"]]
    assert len(tokens) == 1, results
    uid = http("/user/me", tokens[0])["data"]["id"]
    assert sql(f"SELECT phone IS NULL FROM tb_user WHERE id={uid}") == "1"
    bound_uid = int(
        sql(
            "INSERT INTO tb_user(nick_name) VALUES('binding fixture');SELECT LAST_INSERT_ID()"
        )
    )
    compose(
        "redis", ["redis-cli", "HSET", "login:token:" + users[0], "id", str(bound_uid)]
    )
    bound = f"bind-{VOUCHER}@example.test"
    code = send(bound, "bind", users[0])
    body = {"email": bound, "code": code}
    assert not http("/user/email/login", "", body)["success"]
    assert not http("/user/email/bind", users[1], body)["success"]
    assert http("/user/email/bind", users[0], body)["success"]
    assert sql(f"SELECT user_id FROM user_email WHERE email='{bound}'") == str(
        bound_uid
    )
    # Remove only this fixture's cooldown so the bound address can log in immediately.
    compose(
        "redis",
        [
            "redis-cli",
            "DEL",
            "xzdp:"
            + os.getenv("GO_ENV", "test")
            + ":verification:cooldown:"
            + hashlib.sha256(bound.encode()).hexdigest(),
        ],
    )
    code = send(bound, "login")
    login = http("/user/email/login", "", {"email": bound, "code": code})
    assert http("/user/me", login["data"])["data"]["id"] == bound_uid
    sql(
        f"DELETE FROM user_email WHERE user_id IN ({uid},{bound_uid});DELETE FROM tb_user WHERE id IN ({uid},{bound_uid}) AND phone IS NULL"
    )
    print(
        "PASS actual SMTP delivery, cooldown, one-time login, session and purpose/user-bound binding"
    )


if __name__ == "__main__":
    users = sessions(9)
    search_booking(users)
    reward(users[0])
    email(users)
