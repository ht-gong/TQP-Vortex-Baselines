#!/usr/bin/env python3
"""In-repo TPC-H data generator: dbgen -> pipe-delimited text -> parquet.

A self-contained re-implementation of the NDS-H generate + transcode scripts
from NVIDIA/spark-rapids-benchmarks (`nds_h_gen_data.py` local mode,
`nds_h_transcode.py` + `nds_h_schema.py`): same dbgen invocation, same Spark CSV
read with the same explicit schema, same `repartition(200)` and the same
parquet-mr writer defaults (provenance: datagen/README.md). Driven by
datagen/gen_tpch.sh.

  gen_tpch.py dbgen     --scale SF --parallel N --range A,B --out RAW_DIR
                        [--dbgen-dir DIR]
      Runs dbgen chunks A..B of N concurrently (`dbgen -s SF -C N -S i -v Y -f Y`,
      exactly the upstream command) and moves the output into RAW_DIR/<table>/.
      Every chunk also writes the fixed-size nation/region tables (identical
      content); gen_tpch.sh transcodes them only from the batch holding chunk 1.

  spark-submit gen_tpch.py transcode --input RAW_DIR --output PQ_DIR
                        [--tables t1,t2,...] [--log-level WARN]
                        [--compression snappy|zstd|lz4raw] [--dictionary true|false]
                        [--writer-version v1|v2]
      For each table: read RAW_DIR/<table> as '|'-delimited CSV with the fixed
      schema below (17 columns for lineitem etc.: dbgen's trailing '|' yields a
      final all-null `ignore` column), repartition(200), write snappy parquet to
      PQ_DIR/<table>/part-*.parquet.

      The defaults reproduce the NDS-H reference format exactly (snappy,
      dictionary encoding with PLAIN fallback, v1 pages, round-robin shuffle).
      The three format knobs exist for the format profiling pass
      (ablation/format_profile.py); they only change how the same rows are
      encoded, never the rows or their order:
        --compression   parquet codec: snappy (default) | zstd | lz4raw (LZ4_RAW)
        --dictionary    false -> no dictionary pages: PLAIN (v1) or the v2
                        fallbacks DELTA_BINARY_PACKED (ints/decimals/dates) and
                        DELTA_BYTE_ARRAY (strings)
        --writer-version v1 (default) | v2 (data page v2 + delta encodings)
"""
import argparse
import os
import shutil
import subprocess
import sys

TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]
UNSCALED = {"nation", "region"}   # fixed size, one unchunked file (every chunk writes it)


# ----------------------------------------------------------------- dbgen
def cmd_dbgen(args):
    start, end = (int(x) for x in args.range.split(","))
    if not (1 <= start <= end <= int(args.parallel)):
        sys.exit(f"bad --range {args.range}: need 1 <= start <= end <= parallel={args.parallel}")
    if not args.dbgen_dir:
        sys.exit("no dbgen dir: pass --dbgen-dir or set DBGEN_DIR")
    dbgen_dir = os.path.abspath(args.dbgen_dir)
    exe = os.path.join(dbgen_dir, "dbgen")
    if not os.access(exe, os.X_OK):
        sys.exit(f"no dbgen binary at {exe}")
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)

    # One dbgen process per chunk, all at once, run in the dbgen dir (dists.dss
    # is found relative to cwd) with DSS_PATH pointing at a staging dir under
    # RAW_DIR, so the text lands on RAW_DIR's filesystem directly instead of
    # in the dbgen dir (upstream writes there and moves the files afterwards;
    # a 25-chunk SF100 batch is ~13 GB). Same dbgen flags as nds_h_gen_data.py;
    # DSS_PATH is dbgen's own output-directory override and does not affect
    # the data. stderr is kept (it lands in the pipeline log).
    stage = os.path.join(out, "_dbgen_out")
    os.makedirs(stage, exist_ok=True)
    env = dict(os.environ, DSS_PATH=stage)
    procs = []
    for i in range(start, end + 1):
        procs.append(subprocess.Popen(
            ["./dbgen", "-s", str(args.scale), "-C", str(args.parallel),
             "-S", str(i), "-v", "Y", "-f", "Y"],
            cwd=dbgen_dir, env=env, stdout=subprocess.DEVNULL))
    failed = [p.returncode for p in procs if p.wait() != 0]
    if failed:
        sys.exit(f"dbgen failed (return codes {failed})")

    for t in TABLES:
        tdir = os.path.join(out, t)
        os.makedirs(tdir, exist_ok=True)
        names = ([f"{t}.tbl"] if t in UNSCALED
                 else [f"{t}.tbl.{i}" for i in range(start, end + 1)])
        for n in names:
            src = os.path.join(stage, n)
            if not os.path.exists(src):
                sys.exit(f"dbgen produced no {n} in {stage}")
            os.rename(src, os.path.join(tdir, n))
    os.rmdir(stage)
    print(f"dbgen: SF{args.scale} chunks {start}-{end}/{args.parallel} -> {out}")


# ----------------------------------------------------------------- schema
def schemas():
    """Identical to nds_h_schema.get_schemas(): keys as long, money as
    DECIMAL(11,2), sizes/counts as int, dates as date, text as string, plus the
    trailing nullable `ignore` column that absorbs dbgen's terminating '|'."""
    from pyspark.sql.types import (StructType, StructField, LongType, IntegerType,
                                   StringType, DecimalType, DateType)
    L, I, S, D, T = LongType(), IntegerType(), StringType(), DecimalType(11, 2), DateType()

    def st(*cols):
        return StructType([StructField(n, t, False) for n, t in cols]
                          + [StructField("ignore", S, True)])

    return {
        "part": st(("p_partkey", L), ("p_name", S), ("p_mfgr", S), ("p_brand", S),
                   ("p_type", S), ("p_size", I), ("p_container", S),
                   ("p_retailprice", D), ("p_comment", S)),
        "supplier": st(("s_suppkey", L), ("s_name", S), ("s_address", S),
                       ("s_nationkey", L), ("s_phone", S), ("s_acctbal", D),
                       ("s_comment", S)),
        "partsupp": st(("ps_partkey", L), ("ps_suppkey", L), ("ps_availqty", I),
                       ("ps_supplycost", D), ("ps_comment", S)),
        "customer": st(("c_custkey", L), ("c_name", S), ("c_address", S),
                       ("c_nationkey", L), ("c_phone", S), ("c_acctbal", D),
                       ("c_mktsegment", S), ("c_comment", S)),
        "orders": st(("o_orderkey", L), ("o_custkey", L), ("o_orderstatus", S),
                     ("o_totalprice", D), ("o_orderdate", T), ("o_orderpriority", S),
                     ("o_clerk", S), ("o_shippriority", I), ("o_comment", S)),
        "lineitem": st(("l_orderkey", L), ("l_partkey", L), ("l_suppkey", L),
                       ("l_linenumber", I), ("l_quantity", D), ("l_extendedprice", D),
                       ("l_discount", D), ("l_tax", D), ("l_returnflag", S),
                       ("l_linestatus", S), ("l_shipdate", T), ("l_commitdate", T),
                       ("l_receiptdate", T), ("l_shipinstruct", S), ("l_shipmode", S),
                       ("l_comment", S)),
        "nation": st(("n_nationkey", L), ("n_name", S), ("n_regionkey", L),
                     ("n_comment", S)),
        "region": st(("r_regionkey", L), ("r_name", S), ("r_comment", S)),
    }


# ----------------------------------------------------------------- transcode
def cmd_transcode(args):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("tpch datagen - transcode").getOrCreate()
    spark.sparkContext.setLogLevel(args.log_level)

    all_schemas = schemas()
    tables = args.tables.split(",") if args.tables else list(all_schemas)
    bad = [t for t in tables if t not in all_schemas]
    if bad:
        sys.exit(f"unknown table(s) {bad}; valid: {list(all_schemas)}")

    for t in tables:
        df = (spark.read
              .option("delimiter", "|").option("header", "false")
              .option("encoding", "ISO-8859-1")
              .csv(f"{args.input}/{t}", schema=all_schemas[t]))
        # Same physical layout as upstream: 200 round-robin partitions per table
        # per batch, written with Spark's parquet defaults (snappy, parquet-mr).
        df = df.repartition(200)
        # Format knobs. `compression` is Spark's own option; the `parquet.*`
        # options are copied verbatim into the Hadoop conf the parquet-mr
        # writer reads (Spark's newHadoopConfWithOptions), so dictionary and
        # page-version selection reach ParquetOutputFormat directly.
        (df.write
           .format("parquet").mode(args.output_mode)
           .option("compression", args.compression)
           .option("parquet.enable.dictionary", "true" if args.dictionary else "false")
           .option("parquet.writer.version", args.writer_version)
           .save(f"{args.output}/{t}"))
        print(f"transcode: {t} -> {args.output}/{t}  "
              f"[{args.compression} dict={args.dictionary} {args.writer_version}]")
    spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("dbgen", help="run dbgen chunks into RAW_DIR/<table>/")
    g.add_argument("--scale", required=True)
    g.add_argument("--parallel", required=True, type=int)
    g.add_argument("--range", required=True, help='"start,end" inclusive chunk range')
    g.add_argument("--out", required=True)
    g.add_argument("--dbgen-dir", default=os.environ.get("DBGEN_DIR"),
                   help="dir with the dbgen binary and dists.dss (default $DBGEN_DIR)")
    g.set_defaults(fn=cmd_dbgen)

    tr = sub.add_parser("transcode", help="RAW_DIR/<table> text -> PQ_DIR/<table> parquet")
    tr.add_argument("--input", required=True)
    tr.add_argument("--output", required=True)
    tr.add_argument("--tables", default="", help="comma-separated subset (default: all)")
    tr.add_argument("--output-mode", default="overwrite")
    tr.add_argument("--log-level", default="WARN")
    tr.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "lz4raw"])
    tr.add_argument("--dictionary", default="true", type=lambda v: v.lower() in ("1", "true", "yes"))
    tr.add_argument("--writer-version", default="v1", choices=["v1", "v2"])
    tr.set_defaults(fn=cmd_transcode)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
