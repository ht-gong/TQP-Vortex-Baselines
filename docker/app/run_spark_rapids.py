#!/usr/bin/env python
"""Run NDS-H (TPC-H) queries 1-22 on Spark with the RAPIDS GPU accelerator.

Reads parquet tables from --input (one sub-dir per table), registers each as a
temp view, then executes the marker-delimited query stream produced by qgen.
Mirrors nds_h_power.py's stream parsing (incl. multi-statement Q15) but is
self-contained. Per-query wall time + GPU-operator count are reported.

  spark-submit ... run_tpch_queries.py <input_dir> <stream.sql> <out_csv>
"""
import os, re, sys, time
from collections import OrderedDict
from pyspark.sql import SparkSession

INPUT, STREAM, OUT_CSV = sys.argv[1], sys.argv[2], sys.argv[3]
# optional 4th arg: comma-separated template numbers to run (e.g. "9" or "1,2,3")
SUBSET = set(sys.argv[4].split(",")) if len(sys.argv) > 4 and sys.argv[4] else None
# optional 5th arg: append to OUT_CSV instead of overwriting (for per-query runs)
APPEND = len(sys.argv) > 5 and sys.argv[5] == "append"
TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]


def parse_stream(path):
    """{query_name: [sql_statements]} — splits each template's body on ';',
    so Q15 becomes one entry with 3 statements (create view / select / drop)."""
    with open(path) as f:
        stream = f.read()
    pat = re.compile(r'-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)', re.DOTALL)
    out = OrderedDict()
    for num, body in pat.findall(stream):
        # split on ';' and keep fragments that contain real SQL (not pure comments)
        stmts = [s.strip() for s in body.split(";")
                 if re.search(r'\b(select|create|drop|with)\b', s, re.I)]
        out[num] = stmts
    return out


def _code_only(stmt):
    """statement text with leading '--' comment lines removed, lowercased."""
    return "\n".join(l for l in stmt.splitlines()
                     if not l.strip().startswith("--")).strip().lower()


def main():
    spark = SparkSession.builder.appName("NDS-H Power Run (RAPIDS GPU)").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark", spark.version, "| rapids.sql.enabled =",
          spark.conf.get("spark.rapids.sql.enabled", "false"),
          "| plugins =", spark.conf.get("spark.plugins", "<none>"))

    # register tables
    for t in TABLES:
        spark.read.parquet(f"{INPUT}/{t}").createOrReplaceTempView(t)
    print("Registered temp views:", ", ".join(TABLES))

    # Warm-up: trigger GPU init + kernel JIT once so it is NOT counted in any
    # query's measured time. Exercises scan + filter + aggregate on the GPU.
    t0 = time.time()
    spark.sql("select l_returnflag, count(*) c, sum(l_quantity) q "
              "from lineitem where l_orderkey < 100000 group by l_returnflag").collect()
    print(f"GPU warm-up done in {time.time()-t0:.1f}s (excluded from query timings)")

    queries = parse_stream(STREAM)
    if SUBSET:
        queries = OrderedDict((k, v) for k, v in queries.items() if k in SUBSET)
    rows = []
    print(f"\n{'query':10} {'status':8} {'secs':>8} {'gpu_ops':>8} {'result_rows':>12}")
    for num, stmts in queries.items():
        name = f"query{num}"
        t0 = time.time()
        try:
            gpu_ops, nrows = 0, 0
            for stmt in stmts:
                df = spark.sql(stmt)
                code = _code_only(stmt)
                if code.startswith("select") or code.startswith("with"):
                    res = df.collect()
                    nrows = len(res)
                    # read the plan AFTER execution so AQE's finalized (GPU) plan is reflected
                    plan = df._jdf.queryExecution().executedPlan().toString()
                    gpu_ops = sum(1 for ln in plan.splitlines() if "Gpu" in ln)
                else:
                    df.collect()  # DDL (create/drop view)
            dt = time.time() - t0
            print(f"{name:10} {'OK':8} {dt:8.2f} {gpu_ops:8d} {nrows:12d}")
            rows.append((name, "OK", f"{dt:.3f}", gpu_ops, nrows))
            # Force JVM GC so Spark's ContextCleaner deletes this query's shuffle
            # files now, instead of letting scratch accumulate across the session.
            try:
                spark.sparkContext._jvm.System.gc()
                time.sleep(2)
            except Exception:
                pass
        except Exception as e:
            dt = time.time() - t0
            msg = str(e).splitlines()[0][:80]
            print(f"{name:10} {'FAIL':8} {dt:8.2f}   {msg}")
            rows.append((name, "FAIL", f"{dt:.3f}", 0, msg))

    write_header = not (APPEND and os.path.exists(OUT_CSV))
    with open(OUT_CSV, "a" if APPEND else "w") as f:
        if write_header:
            f.write("query,status,seconds,gpu_ops,result_rows_or_error\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")

    ok = [r for r in rows if r[1] == "OK"]
    total = sum(float(r[2]) for r in ok)
    print(f"\n{len(ok)}/{len(rows)} queries OK | total GPU wall time {total:.1f}s | csv -> {OUT_CSV}")
    spark.stop()
    sys.exit(0 if len(ok) == len(rows) else 1)


if __name__ == "__main__":
    main()
