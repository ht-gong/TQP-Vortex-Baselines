#!/usr/bin/env python3
"""Prove two TPC-H parquet datasets are the same data in the same format.

  verify_equivalence.py <parquet_A> <parquet_B> [--raw <tbl_dir>] [--label-a X]
                        [--label-b Y] [--report out.md]

Used to certify that datagen/gen_tpch.sh (in-repo generator) reproduces the
upstream NDS-H reference pipeline (rapids/nds_h_pipeline.sh) exactly. For every
table it checks, and FAILS on any difference in:

  schema        Arrow schema incl. nullability and the Spark schema metadata
                Spark embeds in the footer; parquet schema (physical + logical
                types, repetition levels)
  layout        parquet writer (`created_by`), per-column compression codec and
                encoding sets across all column chunks, number of files, total
                row groups, and the multiset of rows-per-file
  rows          exact multiset equality of all rows (DuckDB `EXCEPT ALL` in both
                directions) -- row ORDER is not compared, since Spark's
                round-robin repartition makes it non-deterministic run to run
  --raw         additionally, that each parquet table equals the dbgen text it
                was transcoded from (typed CSV read, EXCEPT ALL both ways), and
                that the trailing `ignore` column is entirely NULL

Reported but NOT compared (expected to differ between runs): file names (they
contain UUIDs), compressed byte sizes (row order affects compression), the
`_SUCCESS` marker. Exit 0 on full equivalence, 1 otherwise.
"""
import argparse
import glob
import os
import sys
from collections import Counter

import duckdb
import pyarrow.parquet as pq

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp",
          "orders", "lineitem"]
# DuckDB types for the typed read of dbgen's '|' text (mirrors the Spark schema).
RAW_TYPES = {
    "part": [("p_partkey", "BIGINT"), ("p_name", "VARCHAR"), ("p_mfgr", "VARCHAR"),
             ("p_brand", "VARCHAR"), ("p_type", "VARCHAR"), ("p_size", "INTEGER"),
             ("p_container", "VARCHAR"), ("p_retailprice", "DECIMAL(11,2)"),
             ("p_comment", "VARCHAR")],
    "supplier": [("s_suppkey", "BIGINT"), ("s_name", "VARCHAR"), ("s_address", "VARCHAR"),
                 ("s_nationkey", "BIGINT"), ("s_phone", "VARCHAR"),
                 ("s_acctbal", "DECIMAL(11,2)"), ("s_comment", "VARCHAR")],
    "partsupp": [("ps_partkey", "BIGINT"), ("ps_suppkey", "BIGINT"),
                 ("ps_availqty", "INTEGER"), ("ps_supplycost", "DECIMAL(11,2)"),
                 ("ps_comment", "VARCHAR")],
    "customer": [("c_custkey", "BIGINT"), ("c_name", "VARCHAR"), ("c_address", "VARCHAR"),
                 ("c_nationkey", "BIGINT"), ("c_phone", "VARCHAR"),
                 ("c_acctbal", "DECIMAL(11,2)"), ("c_mktsegment", "VARCHAR"),
                 ("c_comment", "VARCHAR")],
    "orders": [("o_orderkey", "BIGINT"), ("o_custkey", "BIGINT"),
               ("o_orderstatus", "VARCHAR"), ("o_totalprice", "DECIMAL(11,2)"),
               ("o_orderdate", "DATE"), ("o_orderpriority", "VARCHAR"),
               ("o_clerk", "VARCHAR"), ("o_shippriority", "INTEGER"),
               ("o_comment", "VARCHAR")],
    "lineitem": [("l_orderkey", "BIGINT"), ("l_partkey", "BIGINT"), ("l_suppkey", "BIGINT"),
                 ("l_linenumber", "INTEGER"), ("l_quantity", "DECIMAL(11,2)"),
                 ("l_extendedprice", "DECIMAL(11,2)"), ("l_discount", "DECIMAL(11,2)"),
                 ("l_tax", "DECIMAL(11,2)"), ("l_returnflag", "VARCHAR"),
                 ("l_linestatus", "VARCHAR"), ("l_shipdate", "DATE"),
                 ("l_commitdate", "DATE"), ("l_receiptdate", "DATE"),
                 ("l_shipinstruct", "VARCHAR"), ("l_shipmode", "VARCHAR"),
                 ("l_comment", "VARCHAR")],
    "nation": [("n_nationkey", "BIGINT"), ("n_name", "VARCHAR"),
               ("n_regionkey", "BIGINT"), ("n_comment", "VARCHAR")],
    "region": [("r_regionkey", "BIGINT"), ("r_name", "VARCHAR"), ("r_comment", "VARCHAR")],
}


class Report:
    def __init__(self):
        self.lines, self.fails = [], []

    def line(self, s=""):
        self.lines.append(s); print(s)

    def check(self, table, what, ok, detail=""):
        mark = "OK  " if ok else "FAIL"
        self.line(f"  [{mark}] {what}" + (f"  {detail}" if detail else ""))
        if not ok:
            self.fails.append(f"{table}: {what} {detail}")


def files(d, t):
    fs = sorted(glob.glob(os.path.join(d, t, "*.parquet")))
    if not fs:
        sys.exit(f"no parquet files under {d}/{t}/")
    return fs


def layout(fs):
    """Aggregate parquet metadata over all files of one table."""
    created, codecs, encs, rg, rows_per_file = set(), {}, {}, 0, Counter()
    arrow_schema, pq_schema, total_rows = None, None, 0
    comp_bytes = uncomp_bytes = 0
    for f in fs:
        pf = pq.ParquetFile(f)
        md = pf.metadata
        created.add(md.created_by)
        if arrow_schema is None:
            # str(ParquetSchema) starts with a "<... object at 0x..>" line; drop it.
            arrow_schema = pf.schema_arrow
            pq_schema = "\n".join(str(pf.schema).splitlines()[1:])
        rows_per_file[md.num_rows] += 1
        total_rows += md.num_rows
        rg += md.num_row_groups
        for i in range(md.num_row_groups):
            g = md.row_group(i)
            for j in range(g.num_columns):
                c = g.column(j)
                name = c.path_in_schema
                codecs.setdefault(name, set()).add(c.compression)
                encs.setdefault(name, set()).update(c.encodings)
                comp_bytes += c.total_compressed_size
                uncomp_bytes += c.total_uncompressed_size
    return dict(created=created, codecs=codecs, encs=encs, row_groups=rg,
                rows_per_file=rows_per_file, n_files=len(fs), rows=total_rows,
                arrow_schema=arrow_schema, pq_schema=pq_schema,
                comp_bytes=comp_bytes, uncomp_bytes=uncomp_bytes)


def except_all(con, a_sql, b_sql):
    return con.execute(f"SELECT count(*) FROM ({a_sql} EXCEPT ALL {b_sql})").fetchone()[0]


def pq_sql(fs):
    lst = ",".join(f"'{f}'" for f in fs)
    return f"SELECT * FROM read_parquet([{lst}])"


def raw_sql(raw_dir, t):
    fs = sorted(glob.glob(os.path.join(raw_dir, t, f"{t}.tbl*")))
    if not fs:
        sys.exit(f"no {t}.tbl* under {raw_dir}/{t}/")
    lst = ",".join(f"'{f}'" for f in fs)
    cols = ", ".join(f"'{n}': '{ty}'" for n, ty in RAW_TYPES[t]) + ", 'ignore': 'VARCHAR'"
    # auto_detect=false: DuckDB's sniffer miscounts columns on dbgen's trailing '|'.
    return (f"SELECT * EXCLUDE (ignore) FROM read_csv([{lst}], delim='|', header=false, "
            f"columns={{{cols}}}, quote='', escape='', auto_detect=false, null_padding=true)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a"); ap.add_argument("b")
    ap.add_argument("--raw", help="dbgen text dir (<raw>/<table>/<table>.tbl*) to compare both against")
    ap.add_argument("--label-a", default="A"); ap.add_argument("--label-b", default="B")
    ap.add_argument("--report", help="also write the report to this file")
    ap.add_argument("--threads", type=int, default=0)
    args = ap.parse_args()

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    rep = Report()
    rep.line(f"# Dataset equivalence: {args.label_a} vs {args.label_b}")
    rep.line(f"- {args.label_a}: {os.path.abspath(args.a)}")
    rep.line(f"- {args.label_b}: {os.path.abspath(args.b)}")
    if args.raw:
        rep.line(f"- raw dbgen text: {os.path.abspath(args.raw)}")

    for t in TABLES:
        rep.line(f"\n## {t}")
        fa, fb = files(args.a, t), files(args.b, t)
        la, lb = layout(fa), layout(fb)

        rep.check(t, "arrow schema (types, nullability, spark metadata)",
                  la["arrow_schema"].equals(lb["arrow_schema"], check_metadata=True),
                  "" if la["arrow_schema"].equals(lb["arrow_schema"], check_metadata=True)
                  else f"\n    A: {la['arrow_schema']}\n    B: {lb['arrow_schema']}")
        rep.check(t, "parquet schema (physical/logical types, repetition)",
                  la["pq_schema"] == lb["pq_schema"])
        rep.check(t, "writer", la["created"] == lb["created"],
                  f"{sorted(la['created'])} vs {sorted(lb['created'])}")
        rep.check(t, "compression codecs per column", la["codecs"] == lb["codecs"],
                  f"{sorted(set().union(*la['codecs'].values()))}")
        enc_ok = la["encs"] == lb["encs"]
        rep.check(t, "encodings per column", enc_ok,
                  "" if enc_ok else "\n" + "\n".join(
                      f"    {c}: {sorted(la['encs'].get(c, []))} vs {sorted(lb['encs'].get(c, []))}"
                      for c in sorted(set(la["encs"]) | set(lb["encs"]))
                      if la["encs"].get(c) != lb["encs"].get(c)))
        rep.check(t, "file count", la["n_files"] == lb["n_files"],
                  f"{la['n_files']} vs {lb['n_files']}")
        rep.check(t, "row groups", la["row_groups"] == lb["row_groups"],
                  f"{la['row_groups']} vs {lb['row_groups']}")
        rep.check(t, "rows per file (multiset)", la["rows_per_file"] == lb["rows_per_file"],
                  f"min/max {min(la['rows_per_file'])}/{max(la['rows_per_file'])}"
                  f" vs {min(lb['rows_per_file'])}/{max(lb['rows_per_file'])}")
        rep.check(t, "total rows", la["rows"] == lb["rows"], f"{la['rows']:,} vs {lb['rows']:,}")
        rep.line(f"  [info] compressed bytes {la['comp_bytes']:,} vs {lb['comp_bytes']:,}"
                 f" (uncompressed {la['uncomp_bytes']:,} vs {lb['uncomp_bytes']:,})"
                 " -- not compared; row order affects compression")
        enc_summary = ", ".join(f"{c}:{'/'.join(sorted(e))}" for c, e in sorted(la["encs"].items()))
        rep.line(f"  [info] encodings: {enc_summary}")

        # exact multiset equality of rows, both directions
        A, B = pq_sql(fa), pq_sql(fb)
        ab, ba = except_all(con, A, B), except_all(con, B, A)
        rep.check(t, "rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0",
                  ab == 0 and ba == 0, f"({ab}, {ba})")

        if args.raw:
            R = raw_sql(args.raw, t)
            A_nz = A.replace("SELECT *", "SELECT * EXCLUDE (ignore)")
            B_nz = B.replace("SELECT *", "SELECT * EXCLUDE (ignore)")
            ra, ar = except_all(con, R, A_nz), except_all(con, A_nz, R)
            rb, br = except_all(con, R, B_nz), except_all(con, B_nz, R)
            rep.check(t, f"raw dbgen text == {args.label_a} parquet", ra == 0 and ar == 0, f"({ra}, {ar})")
            rep.check(t, f"raw dbgen text == {args.label_b} parquet", rb == 0 and br == 0, f"({rb}, {br})")
            nn_a = con.execute(f"SELECT count(ignore) FROM ({A})").fetchone()[0]
            nn_b = con.execute(f"SELECT count(ignore) FROM ({B})").fetchone()[0]
            rep.check(t, "`ignore` column all NULL in both", nn_a == 0 and nn_b == 0,
                      f"non-null: {nn_a}, {nn_b}")

    rep.line("\n## Verdict")
    if rep.fails:
        rep.line(f"NOT EQUIVALENT -- {len(rep.fails)} failed check(s):")
        for f in rep.fails:
            rep.line(f"  - {f}")
    else:
        rep.line(f"EQUIVALENT: {args.label_a} and {args.label_b} hold identical rows in an "
                 "identical parquet schema and physical layout (writer, codec, encodings, "
                 "file/row-group structure).")
    if args.report:
        with open(args.report, "w") as fh:
            fh.write("\n".join(rep.lines) + "\n")
    sys.exit(1 if rep.fails else 0)


if __name__ == "__main__":
    main()
