# TPC-H SF500 — RAPIDS GPU query run (NDS-H queries 1–22)

Power-run of all 22 NDS-H (TPC-H) queries on the SF500 Parquet dataset staged in
the ramdisk (`/dev/shm`), executed on Spark 3.5.8 with the RAPIDS Accelerator on a
**single RTX 5090 (32 GB)**. All 22 ran on the GPU.

## Configuration (what made it work)

- **1 worker / 1 GPU**: Spark `local[*]` — the driver is the single GPU executor.
- **Dataset in ramdisk**: 180 GB Parquet copied into `/dev/shm` (input I/O from RAM).
- **Large host-memory spill**: `spark.rapids.memory.host.spillStorageSize=200g` — GPU
  partitions spill to host RAM first, hitting disk only as a last resort. This is the
  fix for the heavy joins (Q9/Q18/Q21) that otherwise filled the disk.
- **RAPIDS shuffle**: `spark.shuffle.manager=...spark358.RapidsShuffleManager` (MULTITHREADED).
- **Startup excluded from measurement**: each query is run in its own JVM and the timer
  wraps only `collect()`; a GPU warm-up query absorbs first-touch kernel JIT. JVM/GPU
  startup is unmeasured wall-clock. Running one query per JVM also guarantees Spark's
  shuffle scratch is reclaimed between queries (a single long session accumulates it).
- **Disk watchdog**: aborts a query if free disk drops below 30 GB — the box can never
  wedge on a full disk again.
- Other: `concurrentGpuTasks=2`, `shuffle.partitions=1024`, `maxPartitionBytes=1g`,
  `pinnedPool=8g`, driver heap 96 g.

Driver: `/workspace/baseline/rapids/run_tpch_safe.sh` + `run_tpch_queries.py`.
Queries: `/workspace/baseline/tpch_sf500/queries/stream_qualification.sql` (qualification params).

## Results (GPU execution time, startup excluded)

| query | seconds | gpu_ops | result rows |
|-------|--------:|--------:|------------:|
| q1  |  12.84 | 14 | 4 |
| q2  |  36.67 | 92 | 100 |
| q3  |  43.94 | 29 | 10 |
| q4  |  22.87 | 25 | 5 |
| q5  | 100.27 | 68 | 5 |
| q6  |   6.65 |  9 | 1 |
| q7  |  50.73 | 70 | 4 |
| q8  | 121.14 | 88 | 2 |
| q9  | 157.83 | 52 | 175 |
| q10 |  41.08 | 39 | 20 |
| q11 |  30.06 | 86 | 467,405 |
| q12 |  21.48 | 23 | 2 |
| q13 |  19.08 | 22 | 40 |
| q14 |  19.62 | 18 | 1 |
| q15 |  32.41 | 51 | 1 |
| q16 |  23.01 | 33 | 27,840 |
| q17 |  61.23 | 33 | 1 |
| q18 | 124.53 | 50 | 100 |
| q19 |  21.53 | 20 | 1 |
| q20 |  41.13 | 74 | 72,343 |
| q21 |  97.83 | 71 | 100 |
| q22 |  15.81 | 39 | 7 |

**22/22 OK on GPU. Sum of query times ≈ 1102 s (~18.4 min).** Heaviest: q9 (158 s),
q18 (125 s), q8 (121 s), q5 (100 s), q21 (98 s). Result CSV: `query_times_gpu.csv`.

## Notes
- Single 32 GB GPU only (Spark local mode uses one GPU); the second RTX 5090 is idle.
  Using both needs a standalone cluster with two GPU executors.
- Times include host-memory/disk spill overhead inherent to SF500 on one GPU; they are
  a working baseline, **not** an official TPC-H result (NDS-H is non-compliant by license).
- Per-query Spark logs: `/workspace/baseline/tpch_sf500/q<N>.log`.
