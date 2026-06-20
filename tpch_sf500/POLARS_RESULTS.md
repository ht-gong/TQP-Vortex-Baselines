# TPC-H SF500 — Polars query run (queries 1–22)

Same dataset and same logical TPC-H queries as the RAPIDS/Spark run, executed
with **Polars** on the SF500 Parquet staged in the ramdisk (`/dev/shm`). Two
backends were measured: **Polars CPU** (streaming engine) and **Polars GPU**
(cudf-polars / RAPIDS 26.6 on a single RTX 5090).

## Method

- **Data**: the existing 180 GB SF500 Parquet (8 tables) in `/dev/shm/tpch_sf500/parquet`.
- **Queries**: native-Polars implementations of TPC-H q1–22 (`polars/tpch_queries.py`).
  Polars' SQL frontend cannot run the Spark SQL stream directly (correlated/scalar
  subqueries in Q2/Q15/Q17/Q20/Q21/Q22), so the canonical native-Polars expressions
  are used — same logical queries, same standard validation parameters. Q11's
  `FRACTION` is scaled by SF (0.0001/500), matching the qgen stream.
  **All 22 result row-counts match the RAPIDS run exactly.**
- **Timing**: per-query wall time wraps only `collect()`. Polars has no JVM/GPU
  startup to amortize; a warm-up query absorbs first-touch/JIT. One process per
  query (clean scratch / GPU context), incremental CSV, disk watchdog (<30 GB → kill).
- **Numerics**: monetary `Decimal` columns read as `Float64` for robust SF500 sums
  (standard for Polars TPC-H). RAPIDS used decimals — expect tiny last-digit diffs.
- **Host**: 256 cores, 1 TB RAM, 2× RTX 5090 (32 GB). CPU uses 122 threads; GPU
  uses a single 5090.

## GPU configuration (what was needed to fit SF500 on one 32 GB GPU)

cudf-polars' default in-memory path OOMs on SF500 joins. The run uses the
**streaming executor** with **async pool + rapidsmpf device→host spill**
(`RAPIDSMPF_SPILL_DEVICE_LIMIT=22 GB`, `target_partition_size=128 MB`,
`spill_to_pinned_memory=True`) — the cudf-polars analog of the RAPIDS host-spill.
A managed-memory (UVM) variant also works but pages over PCIe and is ~4× slower on
joins (q1 69 s vs 15 s), so async-spill was the default.

**q18 and q21 OOM'd on the async path** (in ~4 s, regardless of partition size):
a single high-cardinality groupby task whose device working set exceeds 32 GB and
which rapidsmpf cannot split mid-operation — not a spill-buffer-capacity problem.
They were re-run on **managed memory (UVM)**, which pages that one task to host
(spill target = full 1 TB host RAM) and completes them, slowly: **q18 168 s, q21 730 s**.

## Results — per-query seconds (startup excluded)

| query | Polars CPU | Polars GPU | RAPIDS GPU (Spark) | rows |
|------:|-----------:|-----------:|-------------------:|-----:|
| q1  |   9.56 |  15.57 |  12.84 | 4 |
| q2  |   2.81 |   2.52 |  36.67 | 100 |
| q3  |   8.97 | 106.62 |  43.94 | 10 |
| q4  |  14.00 |   5.82 |  22.87 | 5 |
| q5  |  10.33 | 164.05 | 100.27 | 5 |
| q6  |   1.12 |   9.52 |   6.65 | 1 |
| q7  |  29.61 | 370.15 |  50.73 | 4 |
| q8  |  20.41 | 146.80 | 121.14 | 2 |
| q9  |  58.35 | 564.03 | 157.83 | 175 |
| q10 |  15.71 |  36.26 |  41.08 | 20 |
| q11 |   3.21 |   2.61 |  30.06 | 467,405 |
| q12 |   6.98 | 111.43 |  21.48 | 2 |
| q13 |  10.00 |   7.72 |  19.08 | 40 |
| q14 |   1.97 |  18.38 |  19.62 | 1 |
| q15 |   3.40 |   7.42 |  32.41 | 1 |
| q16 |   5.96 |   5.73 |  23.01 | 27,840 |
| q17 |   5.20 |  14.72 |  61.23 | 1 |
| q18 |  31.89 | 168.29 †| 124.53 | 100 |
| q19 |   3.65 | 140.95 |  21.53 | 1 |
| q20 |  17.40 |  14.38 |  41.13 | 72,343 |
| q21 |  99.21 | 729.87 †|  97.83 | 100 |
| q22 |   6.76 |   2.27 |  15.81 | 7 |
| **Σ** | **≈366** | **≈2645** | **≈1102** | |

† q18/q21 used managed memory (UVM); all others used the async-spill config.
All 22 GPU queries completed; row counts match the RAPIDS run exactly.

## Findings

- **Ranking on this box (SF500): Polars CPU ≈ 366 s ≪ Spark-RAPIDS GPU ≈ 1102 s ≪
  Polars single-GPU ≈ 2645 s.** The CPU streaming engine (122 threads, 1 TB RAM, no
  spill pressure) is ~3× faster than Spark-RAPIDS and ~7× faster than cudf-polars.
- **A single 32 GB GPU is the wrong tool for SF500.** Once joins exceed device memory
  they spill device→host over PCIe and throughput collapses — q9 9.4 min, q21 12 min,
  q7 6.2 min on cudf-polars. Spark-RAPIDS spills more gracefully (mature host-spill +
  shuffle) so it's ~2.4× faster than cudf-polars overall, but still far behind CPU.
- **cudf-polars is excellent when the query fits the GPU**: q2 2.5 s, q11 2.6 s,
  q22 2.3 s, and it beats CPU on q4/q16/q20. The bimodal profile (a few seconds when
  it fits, many minutes when it spills) is the whole story.
- q21 (~99 s) is the CPU outlier — two `n_unique` passes over the 3 B-row lineitem;
  the same shape is what makes it the GPU's worst case (730 s) and an async-path OOM.

## Takeaway

For SF500 TPC-H on this hardware, **CPU Polars is the engine to use.** The GPUs only
pay off if the working set fits in device memory — which for SF500 means either a
smaller scale factor, or using **both** RTX 5090s with a partitioned/multi-GPU
executor (Spark standalone with 2 executors, or cudf-polars `Cluster.SPMD`) so each
join half fits. That multi-GPU path is the natural next experiment if GPU is the goal.

## Files

- Queries: `polars/tpch_queries.py` · Runner: `polars/run_tpch_polars.py`
- CPU driver: `polars/run_polars.sh` → `query_times_polars.csv`
- GPU driver: `polars/run_polars_gpu.sh` → `query_times_polars_gpu.csv`
- venvs: `polars/.venv` (CPU, polars 1.41) · `polars/.venv-gpu` (cudf-polars 26.6)
