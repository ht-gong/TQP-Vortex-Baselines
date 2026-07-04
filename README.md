# TPC-H GPU baseline — Spark-RAPIDS vs Polars

Benchmark harness and results comparing **GPU and CPU query engines on TPC-H
SF500**, run on a Vast.ai instance with 2× NVIDIA RTX 5090 (Blackwell, 32 GB),
256 cores, 1 TB RAM.

## Engines compared

| Engine | Where |
|--------|-------|
| Spark 3.5.8 + RAPIDS Accelerator 26.04.2 (GPU) | `rapids/` |
| Polars 1.41 — CPU streaming, and cudf-polars 26.6 — GPU | `polars/` |
| Sirius (GPU-native SQL, DuckDB extension, out-of-core) | `sirius/` |
| NDS-H (TPC-H) data + query generator | `rapids/nds_h_pipeline.sh` (+ upstream clones) |

## Layout

```
rapids/        Spark-RAPIDS runner + per-query safe driver + spark config
               (run_tpch_queries.py, run_tpch_safe.sh, activate.sh, conf/)
polars/        native-Polars TPC-H q1-22 + CPU/GPU runners
               (tpch_queries.py, run_tpch_polars.py, run_polars*.sh)
sirius/        Sirius GPU-native SQL engine runner + gpu_execution config + setup
               (setup_sirius.sh, sirius.yaml, run_tpch_sirius.py, run_sirius.sh)
results/    results + the query stream (queries/), CSVs, write-ups, LaTeX table
docker/        self-contained Docker image: Spark-RAPIDS + GPU Polars + NDS-H gen,
               produces the GPU comparison table  (see docker/README.md)
```

Not committed (regenerate / re-download): the 180 GB SF500 parquet
(`results/parquet/`), python venvs, the RAPIDS jar, the conda env, and the
upstream clones (`tpch-kit`, `spark-rapids-benchmarks`, `lancelot`). See
`docker/README.md` to rebuild data + results reproducibly.

## Headline results (SF500, per-query seconds, startup excluded)

| Engine | Total | Notes |
|--------|------:|-------|
| Polars CPU | ~366 s | fastest here — 122 threads, 1 TB RAM, no spill pressure |
| Spark-RAPIDS GPU | ~1102 s | single 32 GB GPU, host-memory spill; all 22 complete |
| Sirius GPU | 439 s* | *17/22 queries; 5 heaviest joins OOM on one 32 GB GPU |
| Polars GPU (cudf-polars) | ~2645 s | single GPU; spill-bound joins dominate |

\*Sirius total is over the **17 queries it completed** (q3/q8/q9/q10/q21 OOM). On
those **same 17 queries**, Sirius runs in **439 s** vs Spark-RAPIDS **640 s** and
Polars-CPU **164 s** — so Sirius is ~1.5× faster than Spark-RAPIDS where it fits,
but Spark-RAPIDS (200 GB host spill store) is the more robust single-GPU engine.

Per-query tables: `results/QUERY_RESULTS.md`, `results/POLARS_RESULTS.md`,
`results/gpu_runtimes.tex`, and the canonical `results/all_results.csv`
(every engine × SF × query). All completed-query result row counts match across
engines.

**Scale-factor sweep — all four engines** (`sirius/run_scale_sweep.sh` +
`run_scale_sweep_all.sh`) runs q1-22 at SF30/50/100/300/500 to show query time vs
data size (headline table also in `AGENTS.md`). Total completed-query seconds:

| SF | Polars-CPU | Sirius GPU | Polars-GPU | Spark-RAPIDS |
|---:|-----------:|-----------:|-----------:|-------------:|
| 30 | 26 | 43 | 34 | 333 |
| 100 | 96 | 91 | 105 | 504 |
| 300 | 243 | 344 (21/22) | 774 | 870 |
| 500 | 366 | 439 (17/22) | 2645 | 1102 |

CPU-Polars wins at every scale. Sirius is fast and complete to SF100, then the
heaviest joins OOM the single 32 GB GPU (a data-size wall, not a bug — they run
fine at smaller SF). Polars-GPU degrades into PCIe spill; Spark-RAPIDS carries
high fixed per-query overhead but completes all 22 at every scale (spilling
gracefully to its 200 GB host store, given ramdisk shuffle scratch).

**Takeaway:** on a single 32 GB GPU at SF500, CPU Polars beats every GPU engine —
the GPUs are bottlenecked spilling large joins to host over PCIe, and the very
heaviest joins even OOM Sirius outright. Multi-GPU (2 executors / `num_gpus: 2`)
is the natural next step.

## Results data — `results/all_results.csv`

The single canonical results file (the **only** results CSV kept — everything
else is a throwaway). One row per `(engine, scale_factor, query)` — 440 rows
(4 engines × 5 SFs × 22 queries). Each runner writes a temp CSV and upserts its
slice via `merge_results.py <engine> <sf> <run_csv>`; every other view (summary,
matrices, per-engine scaling) is a one-line pivot of this file.

| column | type | values / meaning |
|--------|------|------------------|
| `engine` | string | `polars_cpu` (Polars streaming, CPU) · `polars_gpu` (cudf-polars, GPU) · `rapids` (Spark + RAPIDS, GPU) · `sirius` (Sirius, GPU) |
| `scale_factor` | int | TPC-H scale factor: `30`, `50`, `100`, `300`, `500` (≈ GB of raw data) |
| `query` | string | `query1` … `query22` (TPC-H q1–q22) |
| `status` | string | `OK` completed · `FAIL` engine error (a GPU out-of-memory appears here, with an "OOM retry limit" message in `rows_or_error`) · `KILLED_DISK` disk-watchdog killed it (free scratch < threshold) · `TIMEOUT` exceeded the per-query timeout |
| `seconds` | float | per-query wall-clock seconds, **cold** run, engine startup excluded (a warm-up query absorbs JVM/GPU/JIT init). For a failure this is time-to-failure; `NA` for a watchdog kill |
| `rows_or_error` | int / string | result **row count** when `status=OK`; otherwise a short error message |

```csv
engine,scale_factor,query,status,seconds,rows_or_error
polars_cpu,100,query1,OK,2.308,4
sirius,500,query9,FAIL,259.488,INTERNAL Error: ... GPU pipeline task exceeded maximum OOM retry limit (100) for
```

Handy pivots:
```bash
# per (engine, SF): completed count + total OK seconds
duckdb -c "SELECT engine,scale_factor,count(*) FILTER(status='OK') ok,
  round(sum(seconds) FILTER(status='OK'),1) total_s
  FROM 'results/all_results.csv' GROUP BY 1,2 ORDER BY 1,2"
# query × engine_sf seconds matrix
duckdb -c "PIVOT 'results/all_results.csv'
  ON engine||'_sf'||scale_factor USING first(seconds) GROUP BY query"
```
