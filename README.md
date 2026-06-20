# TPC-H GPU baseline — Spark-RAPIDS vs Polars

Benchmark harness and results comparing **GPU and CPU query engines on TPC-H
SF500**, run on a Vast.ai instance with 2× NVIDIA RTX 5090 (Blackwell, 32 GB),
256 cores, 1 TB RAM.

## Engines compared

| Engine | Where |
|--------|-------|
| Spark 3.5.8 + RAPIDS Accelerator 26.04.2 (GPU) | `rapids/` |
| Polars 1.41 — CPU streaming, and cudf-polars 26.6 — GPU | `polars/` |
| NDS-H (TPC-H) data + query generator | `rapids/nds_h_pipeline.sh` (+ upstream clones) |

## Layout

```
rapids/        Spark-RAPIDS runner + per-query safe driver + spark config
               (run_tpch_queries.py, run_tpch_safe.sh, activate.sh, conf/)
polars/        native-Polars TPC-H q1-22 + CPU/GPU runners
               (tpch_queries.py, run_tpch_polars.py, run_polars*.sh)
tpch_sf500/    results + the query stream (queries/), CSVs, write-ups, LaTeX table
docker/        self-contained Docker image: Spark-RAPIDS + GPU Polars + NDS-H gen,
               produces the GPU comparison table  (see docker/README.md)
```

Not committed (regenerate / re-download): the 180 GB SF500 parquet
(`tpch_sf500/parquet/`), python venvs, the RAPIDS jar, the conda env, and the
upstream clones (`tpch-kit`, `spark-rapids-benchmarks`, `lancelot`). See
`docker/README.md` to rebuild data + results reproducibly.

## Headline results (SF500, per-query seconds, startup excluded)

| Engine | Total | Notes |
|--------|------:|-------|
| Polars CPU | ~366 s | fastest here — 122 threads, 1 TB RAM, no spill pressure |
| Spark-RAPIDS GPU | ~1102 s | single 32 GB GPU, host-memory spill |
| Polars GPU (cudf-polars) | ~2645 s | single GPU; spill-bound joins dominate |

Full per-query tables: `tpch_sf500/QUERY_RESULTS.md`,
`tpch_sf500/POLARS_RESULTS.md`, `tpch_sf500/gpu_runtimes.tex`. All 22 result row
counts match across engines.

**Takeaway:** on a single 32 GB GPU at SF500, CPU Polars beats both GPU engines —
the GPUs are bottlenecked spilling large joins to host over PCIe. Multi-GPU
(2 executors) is the natural next step.
