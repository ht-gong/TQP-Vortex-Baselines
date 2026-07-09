#!/usr/bin/env python3
"""Usage: make_parquet.py <sf> [out_dir]"""
import os
import sys
import glob
import shutil

import duckdb


SF = sys.argv[1] if len(sys.argv) > 1 else "100"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = f"{ROOT}/dpfproto/data/tpch"
OUT = sys.argv[2] if len(sys.argv) > 2 else f"{ROOT}/results/parquet"

SCHEMA = {
    "lineitem": [("l_orderkey", "BIGINT"), ("l_partkey", "INTEGER"), ("l_suppkey", "INTEGER"), ("l_linenumber", "BIGINT"), ("l_quantity", "DECIMAL(15,2)"), ("l_extendedprice", "DECIMAL(15,2)"), ("l_discount", "DECIMAL(15,2)"), ("l_tax", "DECIMAL(15,2)"), ("l_returnflag", "VARCHAR"), ("l_linestatus", "VARCHAR"), ("l_shipdate", "DATE"), ("l_commitdate", "DATE"), ("l_receiptdate", "DATE"), ("l_shipinstruct", "VARCHAR"), ("l_shipmode", "VARCHAR"), ("l_comment", "VARCHAR")],
    "orders": [("o_orderkey", "BIGINT"), ("o_custkey", "BIGINT"), ("o_orderstatus", "VARCHAR"), ("o_totalprice", "DECIMAL(15,2)"), ("o_orderdate", "DATE"), ("o_orderpriority", "VARCHAR"), ("o_clerk", "VARCHAR"), ("o_shippriority", "INTEGER"), ("o_comment", "VARCHAR")],
    "customer": [("c_custkey", "BIGINT"), ("c_name", "VARCHAR"), ("c_address", "VARCHAR"), ("c_nationkey", "INTEGER"), ("c_phone", "VARCHAR"), ("c_acctbal", "DECIMAL(15,2)"), ("c_mktsegment", "VARCHAR"), ("c_comment", "VARCHAR")],
    "part": [("p_partkey", "INTEGER"), ("p_name", "VARCHAR"), ("p_mfgr", "VARCHAR"), ("p_brand", "VARCHAR"), ("p_type", "VARCHAR"), ("p_size", "INTEGER"), ("p_container", "VARCHAR"), ("p_retailprice", "DECIMAL(15,2)"), ("p_comment", "VARCHAR")],
    "partsupp": [("ps_partkey", "INTEGER"), ("ps_suppkey", "INTEGER"), ("ps_availqty", "INTEGER"), ("ps_supplycost", "DECIMAL(15,2)"), ("ps_comment", "VARCHAR")],
    "supplier": [("s_suppkey", "INTEGER"), ("s_name", "VARCHAR"), ("s_address", "VARCHAR"), ("s_nationkey", "INTEGER"), ("s_phone", "VARCHAR"), ("s_acctbal", "DECIMAL(15,2)"), ("s_comment", "VARCHAR")],
    "nation": [("n_nationkey", "INTEGER"), ("n_name", "VARCHAR"), ("n_regionkey", "INTEGER"), ("n_comment", "VARCHAR")],
    "region": [("r_regionkey", "INTEGER"), ("r_name", "VARCHAR"), ("r_comment", "VARCHAR")],
}


def src(table):
    for p in (
        f"{BASE}/input{SF}/{table}/{table}.tbl.*",
        f"{BASE}/input{SF}/{table}/{table}.tbl*",
        f"{BASE}/sideways/sf{SF}/{table}/{table}.tbl.*",
        f"{BASE}/sideways/sf{SF}/{table}/{table}.tbl*",
    ):
        if glob.glob(p):
            return p
    raise SystemExit(f"missing {table} tbl files for SF{SF}")


con = duckdb.connect()
con.execute(f"PRAGMA threads={os.cpu_count() or 1}")
os.makedirs(OUT, exist_ok=True)

for table, schema in SCHEMA.items():
    cols = "{" + ",".join(f"'{name}':'{typ}'" for name, typ in schema) + "}"
    out = f"{OUT}/{table}"
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    print(f"{table} -> {out}")
    con.execute(f"""
        COPY (
          SELECT * FROM read_csv('{src(table)}',
            delim='|', header=false, parallel=true, auto_detect=false,
            strict_mode=false, columns={cols})
        ) TO '{out}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE)
    """)

print(f"done {OUT}")
