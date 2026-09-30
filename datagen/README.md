# datagen — in-repo TPC-H data generator

The parquet every engine in this repo benchmarks is produced here. `gen_tpch.sh`
is a self-contained re-implementation of the NDS-H generate → transcode pipeline
from NVIDIA `spark-rapids-benchmarks`: same dbgen, same Spark CSV read, same
schema, same `repartition(200)`, same parquet-mr writer defaults.

| file | purpose |
|------|---------|
| `setup_datagen.sh` | one-time host setup: pinned `tpch-kit` clone (dbgen 2.17.3), `dbgen` built the way the NDS-H `tpch-gen` Makefile builds it, `datagen-venv/` (pyspark 3.5.8, pyarrow, duckdb) with a bundled Temurin JDK 17 |
| `env.sh` | `source` it for the venv, JDK 17 and `spark-submit` on PATH |
| `gen_tpch.sh <SF> <PARALLEL> <BATCH> <out>` | the pipeline: batched dbgen → Spark transcode → merge → delete raw; validates, then publishes `<out>/parquet`. `run.sh` calls this |
| `gen_tpch.py` | `dbgen` and `transcode` sub-commands used by `gen_tpch.sh` |

```bash
datagen/setup_datagen.sh                      # once
datagen/gen_tpch.sh 100 200 25 /data/haotiang/parquet-ablation/sf100   # -> .../sf100/parquet/<table>/
```

## What the pipeline does

1. **dbgen** — `dbgen -s SF -C PARALLEL -S i -v Y -f Y` for the chunks of the
   current batch, all concurrently (the exact upstream invocation), moved into
   `_raw/<table>/`. Every chunk also writes the fixed-size `nation`/`region`;
   only the batch holding chunk 1 transcodes them.
2. **transcode** (one `spark-submit --master local[*]` per batch, as upstream) —
   each table is read as `|`-delimited CSV (`encoding ISO-8859-1`) with the fixed
   NDS-H schema: keys `bigint`, money/quantity `DECIMAL(11,2)`, `p_size` /
   `ps_availqty` / `l_linenumber` / `o_shippriority` `int`, dates `date`, text
   `string`, plus a trailing nullable `ignore` column absorbing dbgen's
   terminating `|` (17 columns for lineitem). Then `repartition(200)` and a plain
   `write.parquet` — Spark/parquet-mr defaults: snappy, dictionary encoding with
   `PLAIN` fallback, v1 pages, 128 MB row groups, all columns nullable.
3. **merge** the 200 part-files per table per batch into
   `_parquet.partial/<table>/`, delete the batch's raw text and temp parquet,
   repeat. Spark's scratch (`SPARK_LOCAL_DIRS`) defaults to `<out>/_work/`, on
   the dataset's disk.
4. **validate** with `results/validate_dataset.py` (writer, in-spec `p_brand`,
   row counts), then rename `_parquet.partial` → `parquet`. A failure anywhere
   leaves no `parquet/` directory, so a half-built dataset never looks finished.

The upstream `saveAsTable(...)` is replaced by `save(path)` (no Derby metastore);
the parquet files are the same. Layout consequence carried over deliberately:
files per table = 200 × number of batches, and rows are shuffled out of dbgen's
key order by the round-robin repartition. Changing either is a format decision,
not a port decision — see `results/GENERATOR.md`.

`RAW_STORE=<dir>` keeps each batch's dbgen text under `<dir>/b<start>-<end>/` and
reuses it on later runs, so several parquet format variants can be transcoded
from one dbgen run (`TRANSCODE_OPTS` picks codec, dictionary and page version;
used by `ablation/format_profile.py`). `<dir>/STAMP` records SF, PARALLEL and
BATCH, and a run with different values fails instead of mixing data.

## Provenance

On 2026-09-22/23 this generator was checked against NVIDIA's NDS-H pipeline
(`spark-rapids-benchmarks@efcfa3f`, the same dbgen binary on both sides, tpch-kit
2.17.3) at SF1, SF10 and SF100: the two produced the same rows, schema and
parquet layout (writer, codecs, encodings, file and row-group counts). Row order
and file names are the only differences, and they also differ between two runs
of the upstream pipeline itself.

## Setup notes

- tpch-kit is pinned by commit (`852ad0a` = TPC-H tools 2.17.3). The NDS-H
  `tpch-gen` Makefile assumes the official toolkit and patches `tpcd.h` by line
  number; `setup_datagen.sh` adds the same `SPARK` profile defines by pattern.
  They do not affect data generation.
- `env.sh` sets `SPARK_LOCAL_IP=127.0.0.1` (local-mode driver bind in
  containers) and unsets `CONTAINER_ID` (Spark would think it is under YARN).
- `rapids/activate.sh` uses this toolchain for the RAPIDS runner.
