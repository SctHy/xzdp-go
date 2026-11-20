#!/usr/bin/env python3
"""Expire one shop in the local Compose stack and release 500 concurrent readers."""
import asyncio
import json
import os
import subprocess
import time
import urllib.request

PORT = int(os.environ.get("API_PORT", 8081))
SHOP = int(os.environ.get("SHOP_ID", 2))
BASE = f"http://127.0.0.1:{PORT}"
KEY = f"xzdp:{os.environ.get('GO_ENV', 'test')}:shop:detail:{SHOP}"
COUNT = 500


def metric(name):
    with urllib.request.urlopen(BASE + "/metrics", timeout=3) as response:
        for line in response.read().decode().splitlines():
            key, _, value = line.partition(" ")
            if key == name:
                return int(value)
    raise RuntimeError(f"missing metric {name}")


def redis(*args):
    return subprocess.check_output(
        ["docker", "compose", "exec", "-T", "redis", "redis-cli", "--raw", *args],
        text=True, timeout=10
    ).strip()


async def run():
    with urllib.request.urlopen(f"{BASE}/shop/{SHOP}", timeout=5) as response:
        assert json.load(response)["data"]["id"] == SHOP
    previous = metric("xzdp_shop_invalidations_total")
    assert redis("PEXPIRE", KEY, "1") == "1", "missing warm L2 fixture"
    assert int(redis("PUBLISH", KEY.rsplit(":", 2)[0] + ":invalidate", KEY)) > 0
    deadline = time.monotonic() + 5
    while metric("xzdp_shop_invalidations_total") == previous or redis("EXISTS", KEY) != "0":
        if time.monotonic() >= deadline:
            raise RuntimeError("invalidation not observed")
        await asyncio.sleep(0.01)
    loads = 'xzdp_shop_db_loads_total{kind="detail"}'
    baseline = metric(loads)
    probes = metric("xzdp_shop_existence_probes_total")
    gate = asyncio.Event()
    connected = asyncio.Event()
    ready = 0

    async def read():
        nonlocal ready
        reader, writer = await asyncio.open_connection("127.0.0.1", PORT)
        try:
            ready += 1
            if ready == COUNT:
                connected.set()
            await gate.wait()
            start = time.perf_counter()
            writer.write(f"GET /shop/{SHOP} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
            await writer.drain()
            header, body = (await reader.read()).split(b"\r\n\r\n", 1)
            assert header.split()[1] == b"200"
            data = json.loads(body)
            assert data["success"] and data["data"]["id"] == SHOP
            return (time.perf_counter() - start) * 1000
        finally:
            writer.close()
            await writer.wait_closed()

    batch = asyncio.gather(*(read() for _ in range(COUNT)))
    await asyncio.wait_for(connected.wait(), timeout=15)
    gate.set()
    latencies = sorted(await asyncio.wait_for(batch, timeout=20))
    delta = metric(loads) - baseline + metric("xzdp_shop_existence_probes_total") - probes
    print(f"responses={COUNT} detail_db_loads={delta} p99_ms={latencies[494]:.2f}")
    assert delta == 1, f"expected exactly one cold fill, got {delta}"


if __name__ == "__main__":
    asyncio.run(run())
