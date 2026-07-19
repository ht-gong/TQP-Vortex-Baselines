#!/usr/bin/env python3
"""Usage: run_tpch_duckdb.py <parquet_dir> <stream.sql> <out_csv> [queries] [runs] [warmups]

DuckDB CPU baseline over the shared TPC-H parquet dataset.

Load modes (DUCKDB_LOAD_MODE):
  tables (default) -- CREATE TABLE AS SELECT from parquet, so the whole dataset
                      is resident in memory before timing starts; load time is
                      excluded from per-query seconds. Needs RAM >= dataset.
  views            -- CREATE VIEW over read_parquet(); parquet scan cost is
                      included in every query's time.

Timing follows the repo results contract (see AGENTS.md): one cold measured run
per query by default (RUNS=1, WARMUPS=0), engine startup and load excluded. With
RUNS>1 the reported seconds is the median of the measured runs.

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

# TPC-H primary keys, applied after load in `tables` mode only when DUCKDB_PK=1.
# Off by default: the ART index build on lineitem (3B rows at SF500) costs a lot
# of time and memory, and no TPC-H query plan uses these indexes for its joins.
PRIMARY_KEYS = {
    "region": "r_regionkey",
    "nation": "n_nationkey",
    "part": "p_partkey",
    "supplier": "s_suppkey",
    "customer": "c_custkey",
    "orders": "o_orderkey",
    "partsupp": "ps_partkey, ps_suppkey",
    "lineitem": "l_orderkey, l_linenumber",
}


def parse_stream(path):
    # split qgen stream
    pat = re.compile(r"-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)", re.S)
    with open(path) as f:
        return {
            int(n): [s.strip() for s in body.split(";")
                     if re.search(r"\b(select|create|drop|with)\b", s, re.I)]
            for n, body in pat.findall(f.read())
        }


def load_dataset(con, parquet, mode, with_pk):
    """Materialise the parquet dataset as in-memory tables, or as views."""
    base = parquet.rstrip("/")
    for t in TABLES:
        glob = f"{base}/{t}/*.parquet"
        t0 = time.time()
        if mode == "views":
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{glob}')")
            print(f"  view {t}", flush=True)
        else:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM parquet_scan('{glob}')")
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"  loaded {t:9s} {n:>13,} rows  {time.time() - t0:7.1f}s", flush=True)

    if mode == "tables" and with_pk:
        for t in TABLES:
            t0 = time.time()
            con.execute(f"ALTER TABLE {t} ADD PRIMARY KEY ({PRIMARY_KEYS[t]})")
            print(f"  pk {t:9s} {time.time() - t0:7.1f}s", flush=True)


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
    mode = os.environ.get("DUCKDB_LOAD_MODE", "tables")
    with_pk = os.environ.get("DUCKDB_PK", "0") == "1"

    # connect to duckdb
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

    print(f"duckdb {duckdb.__version__} sf={sf} threads={threads} mode={mode} "
          f"pk={with_pk} runs={runs} warmups={warmups}", flush=True)
    print(f"loading {parquet} ...", flush=True)
    t0 = time.time()
    load_dataset(con, parquet, mode, with_pk)
    print(f"load complete in {time.time() - t0:.1f}s (excluded from query times)", flush=True)

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
