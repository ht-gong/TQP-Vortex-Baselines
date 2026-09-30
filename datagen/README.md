# datagen — in-repo TPC-H data generator

The parquet every engine in this repo benchmarks is produced here. `gen_tpch.sh`
is a self-contained re-implementation of the NDS-H generate → transcode pipeline
from NVIDIA `spark-rapids-benchmarks` (previously the only generator, driven by
`rapids/nds_h_pipeline.sh` from a checkout that was never part of this repo).
It reproduces that pipeline exactly — same dbgen, same Spark CSV read, same
schema, same `repartition(200)`, same parquet-mr writer defaults — and ships with
the tooling that proves it.

| file | purpose |
|------|---------|
| `setup_datagen.sh` | one-time setup: pinned clones of `tpch-kit` (dbgen 2.17.3) and `spark-rapids-benchmarks` (reference only), builds `dbgen`/`qgen` the way the NDS-H `tpch-gen` Makefile does, creates `datagen-venv/` (pyspark 3.5.8, pyarrow, duckdb) with a bundled Temurin JDK 17 |
| `env.sh` | `source` it for the venv, JDK 17 and `spark-submit` on PATH |
| `gen_tpch.sh <SF> <PARALLEL> <BATCH> <out>` | the pipeline: batched dbgen → Spark transcode → merge → delete raw; validates at the end. Same CLI as `rapids/nds_h_pipeline.sh`; `run.sh` calls this |
| `gen_tpch.py` | `dbgen` and `transcode` sub-commands used by `gen_tpch.sh` (replaces `nds_h_gen_data.py` + `nds_h_transcode.py` + `nds_h_schema.py`) |
| `verify_equivalence.py A B [--raw R]` | proves two datasets are the same data in the same parquet format |
| `verify_engines.sh SF A B out/` | runs every engine's real runner on both datasets and checks they are indistinguishable |
| `compare_engine_runs.py` | the comparison behind `verify_engines.sh` |

```bash
datagen/setup_datagen.sh                      # once
datagen/gen_tpch.sh 100 200 25 /data/haotiang/parquet-ablation/sf100   # -> .../sf100/parquet/<table>/
```

## What the pipeline does

1. **dbgen** — `dbgen -s SF -C PARALLEL -S i -v Y -f Y` for the chunks of the
   current batch, all concurrently (the exact upstream invocation), moved into
   `_raw/<table>/`. `nation`/`region` are only emitted with chunk 1.
2. **transcode** (one `spark-submit --master local[*]` per batch, as upstream) —
   each table is read as `|`-delimited CSV (`encoding ISO-8859-1`) with the fixed
   NDS-H schema: keys `bigint`, money/quantity `DECIMAL(11,2)`, `p_size` /
   `ps_availqty` / `l_linenumber` / `o_shippriority` `int`, dates `date`, text
   `string`, plus a trailing nullable `ignore` column absorbing dbgen's
   terminating `|` (17 columns for lineitem). Then `repartition(200)` and a plain
   `write.parquet` — Spark/parquet-mr defaults: snappy, dictionary encoding with
   `PLAIN` fallback, v1 pages, 128 MB row groups, all columns nullable.
3. **merge** the 200 part-files per table per batch into `parquet/<table>/`,
   delete the batch's raw text and temp parquet, repeat.
4. **validate** with `results/validate_dataset.py` (writer, in-spec `p_brand`,
   row counts); a failure aborts the pipeline.

The upstream `saveAsTable(...)` is replaced by `save(path)` (no Derby metastore);
the parquet files are identical (verified below). Layout consequence carried over
deliberately: files per table = 200 × number of batches, and rows are shuffled
out of dbgen's key order by the round-robin repartition. Changing either is a
format decision, not a port decision — see `results/GENERATOR.md`.

## Verification protocol

Both generators are run with identical arguments (`SF PARALLEL BATCH` chosen so
there are ≥2 batches, exercising the merge path) and a standalone dbgen text dump
is kept as a third reference:

```bash
source datagen/env.sh
rapids/nds_h_pipeline.sh 1 4 2 /dev/shm/verify/upstream_sf1      # reference
datagen/gen_tpch.sh      1 4 2 /dev/shm/verify/ours_sf1          # this repo
python datagen/gen_tpch.py dbgen --scale 1 --parallel 4 --range 1,4 \
       --out /dev/shm/verify/raw_sf1 --dbgen-dir <copy of datagen/dbgen>
python datagen/verify_equivalence.py /dev/shm/verify/upstream_sf1/parquet \
       /dev/shm/verify/ours_sf1/parquet --raw /dev/shm/verify/raw_sf1 \
       --label-a upstream --label-b datagen --report verify_sf1.md
datagen/verify_engines.sh 1 /dev/shm/verify/upstream_sf1/parquet \
       /dev/shm/verify/ours_sf1/parquet /dev/shm/verify/engines_sf1
```

`verify_equivalence.py` fails on any difference, per table, in: Arrow schema
(types, nullability, Spark footer metadata), parquet schema (physical/logical
types, repetition), writer string, per-column codec and encoding sets over all
column chunks, file count, row-group count, rows-per-file multiset, total rows,
and exact multiset row equality (DuckDB `EXCEPT ALL` both ways). With `--raw` it
also checks each parquet table against the typed dbgen text and that `ignore` is
all-NULL. Row *order* and file names are the only things not compared: the
round-robin shuffle makes them non-deterministic between any two runs, including
two runs of upstream itself.

`verify_engines.sh` runs each engine's own runner (`TPCH_MERGE=0`, nothing is
written to `results/all_results.csv`) on both datasets and fails if any engine
returns a different status or result row count for any query, or if engines
disagree with each other.

## Setup notes

- Upstreams are pinned by commit in `setup_datagen.sh`
  (`gregrahn/tpch-kit@852ad0a` = TPC-H tools 2.17.3; `NVIDIA/spark-rapids-benchmarks@efcfa3f`).
  The NDS-H `tpch-gen` Makefile assumes the official toolkit's `makefile.suite`
  and patches `tpcd.h` by line number; `setup_datagen.sh` applies the same
  edits to tpch-kit by pattern. None of them affect data generation (they only
  add the `SPARK` query profile and the `-- Template file:` marker to `qgen`).
- Upstream's `check_build_nds_h()` requires a `tpch-gen-*.jar` even in local
  mode; setup drops an empty placeholder so the reference pipeline runs.
- `env.sh` sets `SPARK_LOCAL_IP=127.0.0.1` (local-mode driver bind in
  containers) and unsets `CONTAINER_ID` (Spark would think it is under YARN).
- `rapids/activate.sh` falls back to `datagen-venv` when the original conda env
  (`rapids/env`) is absent, so the RAPIDS runner shares this toolchain.

## Verification record (2026-09-22)

Reports in `verification/`, produced by the protocol above on an 8×H100 box
(Spark 3.5.8 / parquet-mr 1.13.1, dbgen 2.17.3):

| check | result |
|-------|--------|
| `equivalence_sf1.md` — SF1, `PARALLEL=4 BATCH=2`, with `--raw` | EQUIVALENT, all 8 tables, every check |
| `equivalence_sf10.md` — SF10, `PARALLEL=20 BATCH=8` (3 batches) | EQUIVALENT, all 8 tables, every check |
| `engines_sf1.md` — duckdb_cpu, polars_gpu, rapids, sirius on both SF1 datasets | 22/22 OK everywhere; identical status + row counts per engine across datasets; all engines agree |

Only `results/all_results.csv` timings from the certified SF500 remain
un-reproduced here: that dataset is not on this machine, so it was not diffed
against a regenerated SF500.
