# datagen — in-repo TPC-H data generator

The parquet every engine in this repo benchmarks is produced here. `gen_tpch.sh`
is a self-contained re-implementation of the NDS-H generate → transcode pipeline
from NVIDIA `spark-rapids-benchmarks`: same dbgen, same Spark CSV read, same
schema, same `repartition(200)`, same parquet-mr writer defaults.

| file | purpose |
|------|---------|
| `dataset.sh <SF>` | `make data SF=…`: generate `$DATA_DIR/sf<SF>` if missing (PARALLEL = 2·SF, at least 20; BATCH 25), then validate. `run.sh` calls it too |
| `gen_tpch.sh <SF> <PARALLEL> <BATCH> <out>` | the pipeline: batched dbgen → Spark transcode → merge → delete raw; validates, then publishes `<out>/parquet` |
| `gen_tpch.py` | `dbgen` and `transcode` sub-commands used by `gen_tpch.sh` |
| `env.sh` | `source` it for the Spark toolchain (`$PY`, `$JAVA_HOME`, `$SPARK_HOME` from the image) |
| `export_pth.py <parquet> <SF> <out>` | `make pth SF=…`: the same data as TQP-Vortex `.pth` column files (below) |

The toolchain is part of the image (`docker/Dockerfile`, pins in `versions.env`
and `docker/py.lock`): dbgen from the pinned `tpch-kit` clone (2.17.3) at
`$DBGEN_DIR`, pyspark 3.5.8 / pyarrow / duckdb in the `py` env, Temurin JDK 17.

```bash
make data SF=100            # -> $DATA_DIR/sf100/parquet/<table>/
make validate SF=100
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
  number; the Dockerfile's `dbgen` stage adds the same `SPARK` profile defines by
  pattern and builds with `MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH`. The
  defines do not affect data generation.
- `env.sh` sets `SPARK_LOCAL_IP=127.0.0.1` (local-mode driver bind in
  containers) and unsets `CONTAINER_ID` (Spark would think it is under YARN).
- `rapids/activate.sh` uses this toolchain for the RAPIDS runner.

## `.pth` export for TQP-Vortex

This generator's parquet is the ground truth for TQP-Vortex too. `make pth SF=…`
runs `export_pth.py` on `$DATA_DIR/sf<SF>/parquet` and writes the files
TQP-Vortex's own generator (`tpch-dbgen-tensors`) writes, loadable by its
unmodified loader (`list(torch.jit.load(path).parameters())[0]`):
`$DATA_DIR/pth/SF<SF>-tensor-<COL>.pth`, one TorchScript archive per column,
written with torch 2.6.0 (TQP-Vortex's version), for the 54 columns TQP-Vortex
uses (it skips L_LINENUMBER, L_COMMENT, O_CLERK, P_COMMENT, PS_COMMENT, N_COMMENT,
R_COMMENT). Point TQP-Vortex at them with `TQP_DATA_DIR=$DATA_DIR/pth`.

| parquet | `.pth` tensor |
|---|---|
| keys (`bigint`), `p_size`, `ps_availqty`, `o_shippriority` | `int64 [rows]` |
| `l_quantity` `DECIMAL(11,2)` | `int64 [rows]`, the integral value |
| other `DECIMAL(11,2)` | `float64 [rows]`, unscaled / 100 |
| dates | `int32 [rows]`, days since 1990-01-01 |
| `o_orderstatus`, `l_returnflag`, `l_linestatus` | `int8 [rows]`, the character code |
| other strings | `int8 [rows, width]`, bytes then NUL padding (strncpy) |

Rows are in **parquet order** (part-files in sorted name order), not key order,
so the order is fixed for a dataset on disk but differs between two
generations; `SF<SF>-manifest.json` records the part-file order. TQP-Vortex still
marks each table's primary-key column as sorted, which steers its codec choice.
The export fails on a null, an over-long string, a fractional `l_quantity`, a
date outside 1992–1998, or a non-ASCII value.
