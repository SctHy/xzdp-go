"""Summarize completed pgbench runs. Optionally keep every timed sample as CSV."""

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path
from statistics import median

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("logs", type=Path, nargs="+")
parser.add_argument("--csv", type=Path)
args = parser.parse_args()
pattern = re.compile(r"sample ([01]) (\w+) (mysql|pg) ([1-4]) ([\d.]+) ([\d.]+) ([\d.]+)$")
groups = defaultdict(list)
samples = []
for run, path in enumerate(args.logs, 1):
    log = path.read_text()
    if "--- PASS: TestPostgresShadow" not in log:
        parser.error(f"{path} has no successful integration result")
    seen = set()
    for line in log.splitlines():
        match = pattern.search(line)
        if match:
            stage, case, db, iteration, p50, p95, qps = match.groups()
            sample = stage, case, db, iteration
            if sample in seen:
                parser.error(f"{path} has duplicate sample {sample}")
            seen.add(sample)
            samples.append((run, *match.groups()))
            groups[stage, case, db].append(tuple(map(float, (p50, p95, qps))))
    if len(seen) != 72:
        parser.error(f"{path} has {len(seen)} samples, expected 72")

if args.csv:
    with args.csv.open("w", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("run", "indexed", "workload", "engine", "round", "p50_ms", "p95_ms", "qps"))
        writer.writerows(samples)

print("| 索引组 | 场景 | MySQL P50 / P95 ms | PG P50 / P95 ms | MySQL / PG QPS | PG 的 P95 变化 |")
print("|---|---|---:|---:|---:|---:|")
for stage, case, db in sorted(groups):
    if db != "mysql":
        continue
    my, pg = ([median(column) for column in zip(*groups[stage, case, db])] for db in ("mysql", "pg"))
    print(f"| {stage} | {case} | {my[0]:.2f} / {my[1]:.2f} | {pg[0]:.2f} / {pg[1]:.2f} "
          f"| {my[2]:,.0f} / {pg[2]:,.0f} | {(pg[1] / my[1] - 1) * 100:+.1f}% |")
