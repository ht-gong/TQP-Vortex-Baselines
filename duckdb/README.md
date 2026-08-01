# DuckDB CPU Baseline

`duckdb_cpu` — DuckDB as the CPU reference point against the GPU engines
(Sirius, Polars-GPU, Spark-RAPIDS) and Polars-CPU. Same TPC-H parquet dataset,
same `results/queries/stream_qualification.sql`, same results contract as every
other engine in this repo (see `AGENTS.md`).

## Running

```bash
# all 22 queries at SF500, merged into results/all_results.csv
TPCH_SF=500 TPCH_PARQUET=/root/tpc-h/sf500_parquet ./duckdb/run_duckdb.sh

# a query subset
TPCH_SF=100 TPCH_PARQUET=/dev/shm/tpch_sf100/parquet ./duckdb/run_duckdb.sh "1 6 9"

# keep the temp CSV instead of merging
TPCH_MERGE=0 OUT_CSV=/tmp/duckdb.csv ./duckdb/run_duckdb.sh
```

The runner self-merges into `results/all_results.csv` via `merge_results.py` and
deletes its temp CSV — no per-run/per-SF CSVs, same as the other engines.

## Measurement protocol

Mirrors the local `test.py` flow this baseline came from.

- **Load mode** (`DUCKDB_LOAD_MODE`, default `tables`): the dataset is
  materialised into in-memory DuckDB tables (`CREATE TABLE AS SELECT` from
  parquet), then queries run against resident data. **Load time is excluded**
  from the per-query seconds. Requires RAM ≥ dataset; at SF500 the tables are
  ~800 GB resident.
  - `DUCKDB_LOAD_MODE=views` instead creates views over `read_parquet()`, so
    parquet scan cost lands inside every query — use this to compare against
    engines that stream from parquet.
- **Primary keys** (`DUCKDB_PK`, default `0` = off): no PK/ART indexes are
  built. No TPC-H query plan uses them for its joins, and at SF500 the lineitem
  index build costs substantial time and memory for no query benefit. Set
  `DUCKDB_PK=1` to restore them.
- **Timing** (default `WARMUPS=3 RUNS=1`): three prewarm passes over the query
  set, then one measured run each — the `test.py` protocol. `RUNS>1` reports the
  median of the measured runs.

⚠️ **These seconds are WARM.** The `seconds` contract in `AGENTS.md` is cold, and
that is how `polars_cpu` / `sirius` / `polars_gpu` / `rapids` rows were produced.
`duckdb_cpu` numbers are therefore a lower bound relative to the other engines in
the same table — do not read a `duckdb_cpu` vs `polars_cpu` gap as pure engine
speed. `RUNS=1 WARMUPS=0` produces cold, directly comparable numbers.

Other env knobs: `DUCKDB_THREADS` (defaults to the process CPU affinity, not the
whole box), `DUCKDB_MEMORY_LIMIT`, `DUCKDB_TEMP_DIR` (spill location), `STREAM`.

## Output

One row per query, folded into `results/all_results.csv`:

```csv
engine,scale_factor,query,status,seconds,rows_or_error
duckdb_cpu,500,query1,OK,10.226,4
```

## Getting parquet

```bash
./rapids/nds_h_pipeline.sh 1 2 1 /dev/shm/tpch_sf1     # generate SF1 to ramdisk
./duckdb/make_parquet.sh 100                            # from existing DPFProto tbl data
```
