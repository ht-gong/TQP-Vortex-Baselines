# Sirius GPU SQL engine baseline (TPC-H SF500)

[Sirius](https://github.com/sirius-db/sirius) is a **GPU-native SQL engine** from
NVIDIA + UW-Madison. It loads as a **DuckDB extension** and *transparently*
intercepts plain SQL, running supported operators on the GPU via NVIDIA CUDA-X
(cuDF / RMM / cuCascade). Its recommended path, **`gpu_execution`** ("Super
Sirius"), is **out-of-core**: it streams Parquet through a tiered memory manager
(GPU → pinned host → disk) with automatic partitioning and spilling, so it can
run datasets far larger than VRAM. Consumes query plans via the Substrait format;
unsupported operators fall back to DuckDB CPU.

This baseline runs the **same TPC-H q1-22 stream** the `rapids/` and `polars/`
baselines use (`results/queries/stream_qualification.sql`) so the three engines
are directly comparable on the same SF500 parquet and the same single 32 GB
RTX 5090.

## Paper notes

- Reports ~7× speedup over DuckDB on TPC-H **SF100** at equal $/hour, and 5× on
  1 TB (SF1000) on a DGX Station GB300 (repo README; arXiv 2508.04701).
- Gains come from GPU-resident columnar execution (cuDF kernels) + out-of-core
  tiering, avoiding the JVM/host bottlenecks of CPU engines.
- Standard TPC-H uses no window functions, so every query is GPU-eligible; a plan
  that DuckDB lowers to a cross-product (or that hits an unsupported type such as
  128-bit decimal / nested struct) silently falls back to CPU.

## Layout

```
setup_sirius.sh      install pixi, clone sirius --recurse-submodules, build (Blackwell sm_120)
sirius.yaml          gpu_execution config: 1 GPU, 95% VRAM, 128Gi host tier/NUMA, disk spill
run_tpch_sirius.py   parses the shared query stream, runs each query through the Sirius
                     duckdb binary (transparent GPU), times cold+warm, counts result rows
run_sirius.sh        safe per-query driver: one duckdb process per query + disk watchdog
run_scale_sweep.sh   scale sweep SF30/50/100/300: gen -> clean -> run -> free, per SF;
                     rows land in the canonical ../results/all_results.csv
sirius/              the upstream clone (gitignored; re-create with setup_sirius.sh)
```

Not committed (regenerate): the `sirius/` clone + its `.pixi/` build env, and the
SF500 parquet (`/dev/shm/tpch_sf500/parquet`, staged by `rapids/nds_h_pipeline.sh`).

## Requirements (all satisfied on this box)

| Need | This box |
|------|----------|
| GPU compute capability ≥ 7.5 | RTX 5090 = **12.0** (Blackwell) |
| CUDA 13.x, driver ≥ 580.65.06 | CUDA 13.0, driver **580.82.09** |
| glibc ≥ 2.28, `io_uring` enabled | glibc 2.39, `io_uring_disabled=0` |
| `O_DIRECT`-capable parquet storage | works on **/** and **/dev/shm** |

## Build

```bash
cd /workspace/baseline/sirius
./setup_sirius.sh          # ~pixi env solve + compile; produces sirius/build/release/duckdb
```

The build uses pixi's **default (CUDA 13) environment** — this is what targets
Blackwell (`CUDAARCHS` includes `120a`/`120`). Do **not** build with `-e cuda12`
on this box: it stops at Hopper (sm_90) and GPU ops die with "no kernel image".
`setup_sirius.sh` restricts codegen to sm_120 for speed; widen with
`CUDAARCHS='75-real;80-real;86-real;89-real;90a-real;100f-real;120a-real;120'`.

## Run

Data must already be staged to the ramdisk (same as the other baselines):

```bash
# one-time: regenerate SF500 parquet into /dev/shm (~180 GB) if not present
DRIVER_MEM=120g /workspace/baseline/rapids/nds_h_pipeline.sh 500 1000 25 /dev/shm/tpch_sf500

# run all 22 (or a subset) through Sirius on the GPU
cd /workspace/baseline/sirius
./run_sirius.sh                 # queries 1..22
./run_sirius.sh "1 6 9"         # a subset

# scale-factor sweep: run q1-22 at SF30/50/100/300 to show query time vs scale.
# Generates each SF into the ramdisk, cleans + runs it, then frees it before the
# next (clearing the SF500 ramdisk parquet if it needs the room). All rows land
# in the single canonical table: results/all_results.csv (via merge_results.py)
./run_scale_sweep.sh            # SF 30 50 100 300  (override: ./run_scale_sweep.sh "10 30")
```

Output: results are upserted into `results/all_results.csv` (the one canonical
file; `seconds` = cold-scan time, startup excluded — see the schema in the
top-level `README.md`/`AGENTS.md`). `run_sirius.sh` writes a throwaway temp CSV,
folds it in via `merge_results.py sirius <TPCH_SF> <temp>`, and deletes it. Per-query
and engine logs go to `results/*.log` / `results/*_logs/` (gitignored).

### Methodology (matches rapids/polars)

- **One duckdb process per query** so a heavy query's GPU/spill state can't poison
  the rest, guarded by a **disk watchdog** that kills a query if free `/` drops
  below `MIN_FREE_GB` (Sirius's disk spill tier is also capacity-bounded in
  `sirius.yaml`, a second safety net).
- Each process creates DuckDB **views** over the parquet, runs a tiny **warm-up**
  query to absorb one-time GPU/cuDF kernel JIT (excluded), then times the query.
  Iteration 0 = **cold** (the reported `seconds`, comparable to the rapids/polars
  cold parquet scan); iteration 1 = **warm** (Sirius scan cache).
- Single GPU (`topology.num_gpus: 1`) to match `rapids` (`local[*]`, one GPU) and
  `polars` (`device 0`). For 2-GPU, set `num_gpus: 2` or `CUDA_VISIBLE_DEVICES=0,1`.

## Tuning knobs

- `sirius.yaml` — `memory.gpu.usage_limit_fraction`, `memory.host.capacity_bytes`
  (pinned, per NUMA node), `memory.disk.downgrade_root_dirs` (spill dir).
- Env: `SIRIUS_ITERS` (default 2), `SIRIUS_TIMEOUT` (default 2400s), `MIN_FREE_GB`
  (default 25), `SIRIUS_PARQUET`, `SIRIUS_CONFIG_FILE`.
- To *prove* GPU execution (surface fallbacks as errors instead of silent CPU):
  add `SET enable_duckdb_fallback=false;` — the runner also greps the Sirius log
  and records a `gpu`/`fallback` flag per query in the detail CSV.
