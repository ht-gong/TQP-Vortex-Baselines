#!/usr/bin/env python
"""Upsert ONE engine-run's per-query CSV into results/all_results.csv.

  merge_results.py <engine> <scale_factor> <run_csv> [results_dir]

<run_csv> is the native output of any run_* script — columns
`query,status,seconds,[gpu_ops,]result_rows_or_error`. All existing rows for
(engine, scale_factor) are replaced by the run's rows. `all_results.csv` is the
single canonical results table; runners write a throwaway temp CSV and call this,
so no per-run/per-SF CSVs are kept around. Idempotent; atomic write.
"""
import csv, os, sys

ENGINE_ORDER = ["polars_cpu", "sirius", "polars_gpu", "rapids"]
FIELDS = ["engine", "scale_factor", "query", "status", "seconds", "rows_or_error"]


def main():
    if len(sys.argv) < 4:
        sys.exit("usage: merge_results.py <engine> <scale_factor> <run_csv> [results_dir]")
    engine, sf, run_csv = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    resdir = sys.argv[4] if len(sys.argv) > 4 else "/workspace/baseline/results"
    allcsv = os.path.join(resdir, "all_results.csv")

    # keep every existing row except this (engine, sf) slice
    rows = []
    if os.path.exists(allcsv):
        for r in csv.DictReader(open(allcsv)):
            if r["engine"] == engine and int(r["scale_factor"]) == sf:
                continue
            rows.append(r)

    n = 0
    for r in csv.DictReader(open(run_csv)):
        rows.append({"engine": engine, "scale_factor": sf, "query": r["query"],
                     "status": r["status"], "seconds": r["seconds"],
                     "rows_or_error": r.get("result_rows_or_error", "")})
        n += 1

    def key(r):
        e = ENGINE_ORDER.index(r["engine"]) if r["engine"] in ENGINE_ORDER else len(ENGINE_ORDER)
        q = int("".join(c for c in r["query"] if c.isdigit()) or 0)
        return (e, int(r["scale_factor"]), q)
    rows.sort(key=key)

    os.makedirs(resdir, exist_ok=True)
    tmp = allcsv + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in FIELDS})
    os.replace(tmp, allcsv)
    print(f"merged {engine} sf{sf}: {n} rows -> {allcsv} ({len(rows)} total)")


if __name__ == "__main__":
    main()
