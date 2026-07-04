# TPC-H SF500 dataset (Parquet)

NDS-H (NVIDIA's TPC-H derivative) data at **scale factor 500**, generated on this
box and stored as Parquet. Generated 2026-06-19.

## Layout

```
parquet/<table>/part-*.snappy.parquet     # 8 tables, snappy-compressed
```

| table    | rows           | parquet |
|----------|----------------|---------|
| lineitem | 3,000,028,242  | 117 GB  |
| orders   |   750,000,000  |  32 GB  |
| partsupp |   400,000,000  |  22 GB  |
| customer |    75,000,000  | 6.0 GB  |
| part     |   100,000,000  | 3.3 GB  |
| supplier |     5,000,000  | 399 MB  |
| nation   |            25  | small   |
| region   |             5  | small   |
| **total**|                | **180 GB** |

All deterministic tables match their exact TPC-H SF500 cardinalities; `lineitem`
is ~`6,001,215 × 500` by design (random 1–7 lineitems/order → measured avg 4.000).

## Read it

```python
# source /workspace/baseline/rapids/activate.sh   (Spark 3.5.8 + RAPIDS)
df = spark.read.parquet("/workspace/baseline/results/parquet/lineitem")
```

To read with GPU acceleration, launch Spark with the RAPIDS plugin (see
`/workspace/baseline/rapids/README.md`) and point it at this directory.

## How it was generated

Tooling: `spark-rapids-benchmarks/nds-h` (NDS-H). `dbgen` was built from
`gregrahn/tpch-kit` (TPC-H tools v2.17.3) and dropped into
`nds-h/tpch-gen/target/dbgen/` (local-mode generation uses only the `dbgen`
binary). Driver: `/workspace/baseline/rapids/nds_h_pipeline.sh`.

SF500 raw text is ~512 GB — too large to hold alongside Parquet on a 552 GB disk.
The pipeline therefore works in **10 batches of 100 dbgen chunks** (`parallel=1000`):
each batch generates ~54 GB of raw, transcodes it to Parquet
(`nds_h_transcode.py`), merges the part-files into the consolidated table dirs,
then **deletes the raw batch** before the next. Peak disk stayed ~190 GB.

Regenerate (≈2 h on this 256-core / 2×RTX 5090 box):

```bash
/workspace/baseline/rapids/nds_h_pipeline.sh 500 1000 100 /workspace/baseline/results
```

`pipeline_milestones.log` holds the per-batch timing/disk record from this run.

## Note

NDS-H is derived from TPC-H; per the TPC EULA these results are **not** comparable
to official published TPC-H benchmark results. `dbgen` here is the community
`tpch-kit` (v2.17.3), not the official TPC-H V3.0.1 toolkit — fine for generating
a TPC-H-shaped dataset, but note the provenance if strict spec compliance matters.
This dataset lives on non-volume storage (`workspace_is_volume: false`) and will
**not** survive instance recycle/destroy — copy it off-box if you need to keep it.
