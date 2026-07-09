#!/usr/bin/env python3
"""Usage: run_tpch_duckdb.py <parquet_dir> <stream.sql> <out_csv> [queries] [runs] [warmups]"""
import csv
import os
import re
import sys
import time

import duckdb


TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]


def parse_stream(path):
    # split qgen stream
    pat = re.compile(r"-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)", re.S)
    with open(path) as f:
        return {
            int(n): [s.strip() for s in body.split(";")
                     if re.search(r"\b(select|create|drop|with)\b", s, re.I)]
            for n, body in pat.findall(f.read())
        }


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
    runs = int(sys.argv[5]) if len(sys.argv) > 5 else int(os.environ.get("RUNS", "5"))
    warmups = int(sys.argv[6]) if len(sys.argv) > 6 else int(os.environ.get("WARMUPS", "1"))
    sf = os.environ.get("TPCH_SF", "500")

    # connect to duck db
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(os.environ.get('DUCKDB_THREADS', os.cpu_count() or 1))}")
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"PRAGMA memory_limit='{mem}'")

    # parquet tables as views
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{parquet.rstrip('/')}/{t}/*.parquet')")

    queries = parse_stream(stream)
    with open(out_csv, "a", newline="") as f:
        w = csv.writer(f)
        if not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0:
            w.writerow(["engine", "scale_factor", "query", "run", "status", "seconds", "rows_or_error"])

        for q in subset:
            q = int(q)

            # warmup runs
            for _ in range(warmups):
                run_query(con, queries[q])

            # measured runs
            for i in range(1, runs + 1):
                t0 = time.time()
                try:
                    rows = run_query(con, queries[q])
                    dt = time.time() - t0
                    w.writerow(["duckdb_cpu", sf, f"query{q}", i, "OK", f"{dt:.6f}", rows])
                    print(f"query{q} run={i} OK {dt:.3f}s rows={rows}")
                except Exception as e:
                    dt = time.time() - t0
                    msg = str(e).splitlines()[0][:120]
                    w.writerow(["duckdb_cpu", sf, f"query{q}", i, "FAIL", f"{dt:.6f}", msg])
                    print(f"query{q} run={i} FAIL {dt:.3f}s {msg}")
                f.flush()


if __name__ == "__main__":
    main()
