#!/usr/bin/env python
"""Run native-Polars TPC-H queries 1-22 on the SF500 parquet (from ramdisk).

Mirrors the RAPIDS run: per-query wall time excludes engine startup (Polars has
no JVM/GPU startup; a warm-up query absorbs any first-touch/JIT cost), the
dataset is read from /dev/shm, and results are written incrementally to a CSV so
a watchdog kill never loses prior rows.

  run_tpch_polars.py <input_dir> <out_csv> [SUBSET] [append] [engine]
    SUBSET  comma-separated query numbers, e.g. "9" or "1,2,3"  (default: 1..22)
    engine  "streaming" (default, spills to disk) or "in-memory"
"""
import os
import sys
import time
import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tpch_queries import QUERIES

INPUT = sys.argv[1]
OUT_CSV = sys.argv[2]
SUBSET = [int(x) for x in sys.argv[3].split(",")] if len(sys.argv) > 3 and sys.argv[3] else list(range(1, 23))
APPEND = len(sys.argv) > 4 and sys.argv[4] == "append"
ENGINE = sys.argv[5] if len(sys.argv) > 5 else "streaming"


def build_engine():
    """For CPU return the engine string; for GPU build a GPUEngine that spills
    device->host (so SF500 fits one 32 GB GPU) using managed memory + the
    streaming executor with modest partitions."""
    if ENGINE != "gpu":
        return ENGINE
    part_mb = int(os.environ.get("GPU_PART_MB", "256"))
    mode = os.environ.get("GPU_MR", "async")  # "async" (rapidsmpf spill) or "managed" (UVM)
    kw = dict(
        device=0,
        executor="streaming",
        executor_options={
            "fallback_mode": "warn",
            "target_partition_size": part_mb * 1024 * 1024,
            "spill_to_pinned_memory": True,
        },
    )
    if mode == "managed":
        import rmm
        # Unified memory: oversubscribe into host RAM (robust but PCIe-paging slow).
        kw["memory_resource"] = rmm.mr.PrefetchResourceAdaptor(rmm.mr.ManagedMemoryResource())
    # else: default CudaAsyncMemoryResource; the streaming executor spills
    # device->host via rapidsmpf when device pressure exceeds its threshold.
    return pl.GPUEngine(**kw)

TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]
# Monetary columns stored as Decimal -> read as Float64 for robust large sums.
DECIMAL_COLS = {"l_quantity", "l_extendedprice", "l_discount", "l_tax",
                "o_totalprice", "ps_supplycost", "c_acctbal",
                "s_acctbal", "p_retailprice"}


def scan(table):
    lf = pl.scan_parquet(f"{INPUT}/{table}/*.parquet")
    cols = lf.collect_schema().names()
    exprs = []
    for c in cols:
        if c == "ignore":
            continue
        exprs.append(pl.col(c).cast(pl.Float64) if c in DECIMAL_COLS else pl.col(c))
    return lf.select(exprs)


def main():
    print(f"Polars {pl.__version__} | engine={ENGINE} | threads={pl.thread_pool_size()}")
    engine = build_engine()
    lf = {t: scan(t) for t in TABLES}

    # Warm-up: touch lineitem so first-read/JIT is not charged to any query.
    t0 = time.time()
    lf["lineitem"].select(pl.len()).collect(engine=engine)
    print(f"warm-up done in {time.time()-t0:.1f}s (excluded from query timings)")

    write_header = not (APPEND and os.path.exists(OUT_CSV))
    f = open(OUT_CSV, "a" if APPEND else "w")
    if write_header:
        f.write("query,status,seconds,result_rows_or_error\n")
        f.flush()

    print(f"\n{'query':10} {'status':8} {'secs':>9} {'rows':>10}")
    ok = 0
    for n in SUBSET:
        name = f"query{n}"
        t0 = time.time()
        try:
            res = QUERIES[n](lf).collect(engine=engine)
            dt = time.time() - t0
            nrows = res.height
            print(f"{name:10} {'OK':8} {dt:9.2f} {nrows:10d}")
            f.write(f"{name},OK,{dt:.3f},{nrows}\n")
            ok += 1
        except Exception as e:
            dt = time.time() - t0
            msg = str(e).splitlines()[0][:90].replace(",", ";")
            print(f"{name:10} {'FAIL':8} {dt:9.2f}   {msg}")
            f.write(f"{name},FAIL,{dt:.3f},{msg}\n")
        f.flush()
    f.close()

    print(f"\n{ok}/{len(SUBSET)} queries OK | csv -> {OUT_CSV}")
    sys.exit(0 if ok == len(SUBSET) else 1)


if __name__ == "__main__":
    main()
