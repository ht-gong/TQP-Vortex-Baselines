#!/usr/bin/env python3
"""Usage: plot_results.py [runs_csv] [out_dir]"""
import csv
import os
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
import matplotlib.pyplot as plt


runs_csv = sys.argv[1] if len(sys.argv) > 1 else "duckdb/results/duckdb_runs.csv"
out_dir = sys.argv[2] if len(sys.argv) > 2 else "duckdb/results"
golap_dir = Path("dpfproto/logs/golap_ramdisk/20260709_164701")

rows = defaultdict(list)
meta = {}

with open(runs_csv) as f:
    for r in csv.DictReader(f):
        if r["status"] == "OK":
            rows[r["query"]].append(float(r["seconds"]))
            meta[r["query"]] = r

summary = []
for q in sorted(rows, key=lambda x: int(x.replace("query", ""))):
    xs = rows[q]
    summary.append([q, meta[q]["scale_factor"], len(xs), statistics.median(xs), min(xs), max(xs)])

os.makedirs(out_dir, exist_ok=True)
summary_csv = f"{out_dir}/duckdb_summary.csv"
with open(summary_csv, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["query", "scale_factor", "runs", "median_s", "min_s", "max_s"])
    w.writerows([q, sf, n, f"{med:.6f}", f"{lo:.6f}", f"{hi:.6f}"] for q, sf, n, med, lo, hi in summary)

labels = [r[0].replace("query", "Q") for r in summary]
medians = [r[3] for r in summary]

plt.figure(figsize=(7, 4))
plt.bar(labels, medians)
plt.ylabel("median seconds")
plt.title("DuckDB CPU TPC-H SF100")
plt.tight_layout()
plot_png = f"{out_dir}/duckdb_median_runtime.png"
plt.savefig(plot_png, dpi=200)

print(summary_csv)
print(plot_png)

if golap_dir.exists():
    golap = {}
    for path in golap_dir.glob("q*.txt"):
        text = path.read_text(errors="replace")
        m = re.search(r"^time:\s+([0-9.]+)\s+msec", text, re.M)
        golap[path.stem.replace("q", "query")] = float(m.group(1)) / 1000 if m else None

    compare = f"{out_dir}/duckdb_vs_golap_sf100.csv"
    with open(compare, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "duckdb_median_s", "golap_s", "golap_speedup_vs_duckdb"])
        for q, _, _, med, _, _ in summary:
            g = golap.get(q)
            w.writerow([q, f"{med:.6f}", "" if g is None else f"{g:.6f}", "" if not g else f"{med / g:.3f}"])

    x = range(len(summary))
    plt.figure(figsize=(8, 4))
    plt.bar([i - 0.2 for i in x], [r[3] for r in summary], width=0.4, label="DuckDB CPU")
    plt.bar([i + 0.2 for i in x], [golap.get(r[0]) or 0 for r in summary], width=0.4, label="GOLAP GPU")
    plt.xticks(list(x), [r[0].replace("query", "Q") for r in summary])
    plt.ylabel("seconds")
    plt.title("DuckDB CPU vs GOLAP SF100")
    plt.legend()
    plt.tight_layout()
    compare_png = f"{out_dir}/duckdb_vs_golap_sf100.png"
    plt.savefig(compare_png, dpi=200)
    print(compare)
    print(compare_png)
