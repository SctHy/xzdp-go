"""Prepare private local k6 credentials and verify final SQL stock and order uniqueness."""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

from sale import STOCK, USERS, VOUCHER, sessions, sql


def prepare(path):
    sql(f"""INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time)
            VALUES({VOUCHER},{STOCK},NOW()-INTERVAL 1 MINUTE,NOW()+INTERVAL 1 HOUR)""")
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as tmp:
        json.dump({"voucher": str(VOUCHER), "stock": STOCK, "users": sessions(USERS)}, tmp)
    os.replace(tmp.name, path)
    print(f"Prepared {USERS} local users for voucher {VOUCHER}")


def verify(path):
    fixture = json.loads(path.read_text())
    voucher, stock = int(fixture["voucher"]), int(fixture["stock"])
    until = time.monotonic() + 60
    while time.monotonic() < until:
        row = sql(f"""SELECT COUNT(*),COUNT(DISTINCT user_id),
                      (SELECT stock FROM tb_seckill_voucher WHERE voucher_id={voucher})
                      FROM tb_voucher_order WHERE voucher_id={voucher}""")
        if row.split() == [str(stock), str(stock), "0"]:
            print(f"PASS {stock} distinct buyers and orders, stock=0")
            return
        time.sleep(0.3)
    raise AssertionError(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["prepare", "verify"])
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    (prepare if args.operation == "prepare" else verify)(args.file)
