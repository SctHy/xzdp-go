"""Verify incremental geo indexing using one disposable shop in the Compose stack."""

import json
import urllib.request
from lifecycle import poll
from sale import PORT, VOUCHER, sql


def hits(category, x):
    url = f"http://127.0.0.1:{PORT}/shop/of/type/geo?typeId={category}&current=1&x={x}&y=30&dist=100"
    with urllib.request.urlopen(url, timeout=3) as response:
        result = json.load(response)
    assert result["success"], result
    return [b["id"] for b in result["data"]]


def run():
    a = 1000000 + VOUCHER % 1000000
    b = a + 1
    sql(f"""INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score)
        VALUES({VOUCHER},'integration',{a},'','fixture',120,30,0,0,0)""")
    try:
        poll(lambda: VOUCHER in hits(a, 120))
        for _ in range(10):
            assert VOUCHER in hits(a, 120)
        sql(f"UPDATE tb_shop SET type_id={b},x=121 WHERE id={VOUCHER}")
        poll(lambda: VOUCHER in hits(b, 121))
        assert VOUCHER not in hits(a, 120)
        assert VOUCHER not in hits(b, 120)
    finally:
        sql(f"DELETE FROM tb_shop WHERE id={VOUCHER}")
    poll(lambda: not hits(b, 121))
    print("PASS create, ten nearby reads, move, recategorize, delete")


if __name__ == "__main__":
    run()
