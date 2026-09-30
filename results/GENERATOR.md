# Data generator — one source for every engine

**Invariant: every engine runs the exact same parquet, produced by the NDS-H
generator.** A cross-engine number is only meaningful if all engines read
byte-identical input. This document defines the one allowed generator, records a
provenance audit that found the invariant had been violated, and gives the guard
(`validate_dataset.py`) that now enforces it.

## The one allowed generator

TPC-H parquet is produced **only** by the repo's `datagen/gen_tpch.sh`, a
re-implementation of the NDS-H (NVIDIA `spark-rapids-benchmarks`) pipeline:

1. `gen_tpch.py dbgen` — TPC-H **dbgen 2.17.3** (built from the pinned
   `gregrahn/tpch-kit` clone, as the original datasets were) → pipe-delimited `.tbl`.
2. `gen_tpch.py transcode` — Spark 3.5.8 reads the `.tbl` with the fixed NDS-H
   schema and writes snappy parquet (writer `parquet-mr 1.13.1`), one 17-column
   table per dir with a trailing `ignore` column absorbing dbgen's dangling `|`.

Driver: `datagen/gen_tpch.sh <SF> <PARALLEL> <BATCH> <out>`. It was checked
against the upstream NDS-H pipeline it replaces (same rows, schema and parquet
layout; provenance note in `datagen/README.md`). Output layout — the layout
every runner reads:

```
<out>/parquet/<table>/part-NNNNN-<uuid>-c000.snappy.parquet
```

**Forbidden:** parquet written by any other tool — in particular DuckDB's own
`CALL dbgen(...)` export (16 columns, no `ignore`, writer `DuckDB …`) and cudf
exports (writer `cudf …`). Self-generated data is not comparable to the external
standard and has produced materially different query results (below). Do not
point a runner at self-written parquet, even as a convenience.

## Provenance audit (2026-07-25)

Auditing the datasets behind `all_results.csv` found the invariant broken: only
**SF500** was clean external NDS-H. The other scale factors had run on
self-generated or corrupt parquet.

| SF | dataset used | writer | verdict |
|---:|--------------|--------|---------|
| 30 | `sf30_parquet` | cudf 24.10 | ✗ wrong generator |
| 50 | `sf50_parquet` | DuckDB v1.5.4 | ✗ self-generated |
| 100 | `sf100_canon_parquet` | DuckDB v1.5.4 | ✗ self-generated |
| 300 | `sf300_canon_parquet` | DuckDB v1.5.4 | ✗ self-generated |
| 500 | `sf500_parquet` | parquet-mr 1.13.1 | ✓ clean NDS-H |
| 700 | `sf700_parquet` | parquet-mr 1.13.1 | ✓ clean NDS-H (DuckDB-only; >int32 for the GPU engines) |

Two distinct defects:

- **Wrong generator (SF30/50/100/300).** These ran on cudf- or DuckDB-written
  parquet, not the Spark transcode.

- **Corrupt legacy NDS-H `part` (SF100/SF300).** The *real* NDS-H builds for
  SF100/SF300 that exist on disk are themselves broken: `part.p_brand` holds 80
  contiguous values `Brand#11..Brand#90` instead of the 25 the TPC-H spec allows
  (`Brand#<1-5><1-5>`). That inflates distinct `(p_brand, p_type, p_size)` from
  187,500 to ~600,000 and changes the answers to q16/q17/q19 (q16 → 91,640 rows
  vs the correct 27,840). This is a stale artifact from an older dbgen; the
  current `tpch-gen` dbgen does **not** reproduce it — verified with
  `dbgen -s 300 -C 600 -S {1,599}`, which yields exactly the 25 spec brands. So
  the pipeline code is fine; the on-disk SF100/SF300 NDS-H parquet must simply be
  regenerated.

Consequence: `all_results.csv` now keeps **only the certified-clean SF500**
slice (all five engines). The SF30/50/100/300 rows are moved to
`results/archive/` — internally consistent, but not from the required generator.
Restoring them means regenerate-clean + revalidate + re-run (below).

## Guard — `results/validate_dataset.py`

```
python3 results/validate_dataset.py <parquet_dir> <scale_factor>
```

Exits non-zero unless the dataset is clean external NDS-H. It checks:

- **writer** is `parquet-mr` (rejects DuckDB/cudf self-writes);
- **`part.p_brand`** is within the 25-value spec set, and
  `distinct(p_brand,p_type,p_size) ≤ 187,500` (rejects the SF100/SF300 corruption);
- **row counts** match the TPC-H per-SF formula.

`datagen/gen_tpch.sh` runs this automatically after generation and fails
loudly if the fresh dataset does not pass. Run it by hand before trusting any
dataset you did not just generate.

## Regenerating SF30/50/100/300 to restore them

```bash
# generate + validate (PARALLEL≈2·SF, BATCH 25), then run every engine at that SF;
# each engine self-merges into all_results.csv
./run.sh 300            # generates $DATA_DIR/sf300 if missing, validates, runs
```

(GPU engines: int32 caps ≈ SF300.)
