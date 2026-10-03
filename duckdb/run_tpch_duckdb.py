#!/usr/bin/env python3
"""Usage: run_tpch_duckdb.py <parquet_dir> <stream.sql> <out_csv> [queries] [runs] [warmups]

DuckDB CPU baseline over the shared TPC-H parquet dataset.

Every table is a view over read_parquet() of the ramdisk copy, and DuckDB's
external file cache is off, so every query reads and decodes its parquet from
the ramdisk; nothing is loaded into memory ahead of the timed run.

One measured run per query by default (RUNS=1, WARMUPS=0), engine startup
excluded. With RUNS>1 the reported seconds is the median of the measured runs.

Emits the merge_results.py input schema -- `query,status,seconds,
result_rows_or_error`, one row per query -- so run_duckdb.sh can fold it into
results/all_results.csv.
"""
import csv
import os
import re
import sys
import time
from statistics import median

import duckdb


TABLES = ["region", "nation", "supplier", "customer",
          "part", "partsupp", "orders", "lineitem"]


def parse_stream(path):
    # split qgen stream
    pat = re.compile(r"-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)", re.S)
    with open(path) as f:
        return {
            int(n): [s.strip() for s in body.split(";")
                     if re.search(r"\b(select|create|drop|with)\b", s, re.I)]
            for n, body in pat.findall(f.read())
        }


def create_views(con, parquet):
    """One view per table over read_parquet() of the parquet dataset."""
    base = parquet.rstrip("/")
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{base}/{t}/*.parquet')")


def connect():
    """In-memory connection with the benchmark settings; returns (con, threads)."""
    con = duckdb.connect()
    # honour cgroup/taskset affinity -- os.cpu_count() reports the whole box and
    # would oversubscribe when the container is pinned to a subset.
    default_threads = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else (os.cpu_count() or 1)
    threads = int(os.environ.get("DUCKDB_THREADS", default_threads))
    con.execute(f"PRAGMA threads={threads}")
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"PRAGMA memory_limit='{mem}'")
    if os.environ.get("DUCKDB_TEMP_DIR"):
        con.execute(f"PRAGMA temp_directory='{os.environ['DUCKDB_TEMP_DIR']}'")
    # no in-memory copy of parquet file bytes across queries: every query reads
    # the ramdisk
    con.execute("SET enable_external_file_cache = false")
    return con, threads


def run_query(con, stmts):
    # return final select row count
    rows = 0
    for stmt in stmts:
        res = con.execute(stmt)
        code = re.sub(r"(?m)^\s*--.*\n?", "", stmt).strip().lower()
        if code.startswith(("select", "with")):
            rows = len(res.fetchall())
        else:
            res.fetchall()
    return rows


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)

    # get args
    parquet, stream, out_csv = sys.argv[1:4]
    subset = sys.argv[4].replace(",", " ").split() if len(sys.argv) > 4 and sys.argv[4] else range(1, 23)
    runs = int(sys.argv[5]) if len(sys.argv) > 5 else int(os.environ.get("RUNS", "1"))
    warmups = int(sys.argv[6]) if len(sys.argv) > 6 else int(os.environ.get("WARMUPS", "0"))
    sf = os.environ.get("TPCH_SF", "500")

    con, threads = connect()

    print(f"duckdb {duckdb.__version__} sf={sf} threads={threads} views over {parquet} "
          f"runs={runs} warmups={warmups}", flush=True)
    create_views(con, parquet)

    queries = parse_stream(stream)
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "status", "seconds", "result_rows_or_error"])

        for q in subset:
            q = int(q)
            try:
                # optional warmups, then the measured run(s)
                for _ in range(warmups):
                    run_query(con, queries[q])

                times, rows = [], 0
                for _ in range(runs):
                    t0 = time.time()
                    rows = run_query(con, queries[q])
                    times.append(time.time() - t0)

                dt = median(times)
                w.writerow([f"query{q}", "OK", f"{dt:.3f}", rows])
                print(f"query{q:<3d} OK   {dt:8.3f}s rows={rows}", flush=True)
            except Exception as e:
                msg = str(e).splitlines()[0][:120]
                w.writerow([f"query{q}", "FAIL", "NA", msg])
                print(f"query{q:<3d} FAIL {msg}", flush=True)
            f.flush()


if __name__ == "__main__":
    main()
