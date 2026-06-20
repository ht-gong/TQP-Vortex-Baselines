# Multi-tool GPU benchmark image — Spark-RAPIDS + Polars + Lancelot

One image with **all three engines**:

- **TPC-H** on GPU: Spark 3.5.8 + RAPIDS 26.04.2, and cudf-polars 26.6
- **SSB**: Lancelot (hybrid CPU/multi-GPU DBMS, CUDA C++)
- Data generators for both: NDS-H (TPC-H) and SSB dbgen/loader
- CUDA 12.9 **devel** base (nvcc, for Lancelot) + OpenJDK 17 + Intel TBB + NCCL + PAPI

The Dockerfile clones the three upstreams (tpch-kit, spark-rapids-benchmarks,
Lancelot) and downloads Spark + the RAPIDS jar, so the build context is just the
runners + dispatcher.

> They run **one at a time** — each engine saturates the GPU, so concurrent runs
> only contend for VRAM. TPC-H and SSB are different benchmarks/data, so there is
> no single cross-all comparison; the TPC-H pair (Spark-RAPIDS vs Polars) does
> produce a direct comparison table.

## Requirements (host)

Docker + the **NVIDIA Container Toolkit** (`--gpus all`). Not runnable inside an
unprivileged container (no Docker-in-Docker). Tuned for RTX 5090 (sm_120);
override `SM_ARCH`/`NUM_GPU` for other GPUs.

## Quick start

```bash
cd docker-all
./host.sh build                          # docker build -t tpch-multitool .

# quick self-tests (small)
./host.sh tpch-smoke                      # TPC-H SF1 on both GPU engines + table
./host.sh ssb-smoke                       # SSB SF1 gen + build + run
./host.sh smoke                           # both

# TPC-H at scale (Spark-RAPIDS + Polars GPU + comparison table)
SCALE=100 ./host.sh tpch-all

# SSB / Lancelot at scale (1|10|20|40)
SF=10 ./host.sh ssb-all
```

### Plain `docker` (what `host.sh` wraps)

`host.sh` is just convenience; the explicit commands are:

```bash
# build (run from this docker-all/ directory)
docker build -t tpch-multitool .

# self-test
docker run --rm -it --gpus all --shm-size=64g \
  -v "$PWD/data:/data" tpch-multitool tpch-smoke

# TPC-H at SF100 (Spark-RAPIDS + Polars GPU + comparison table)
docker run --rm -it --gpus all --shm-size=64g \
  -e SCALE=100 -v "$PWD/data:/data" tpch-multitool tpch-all

# SSB / Lancelot at SF10 on 2 GPUs, Blackwell arch
docker run --rm -it --gpus all --shm-size=64g \
  -e SF=10 -e NUM_GPU=2 -e SM_ARCH=120 \
  -v "$PWD/data:/data" tpch-multitool ssb-all

# any single command, e.g. just the Polars GPU run:
docker run --rm -it --gpus all --shm-size=64g \
  -e SCALE=100 -v "$PWD/data:/data" tpch-multitool tpch-polars
```

Flags that matter: **`--gpus all`** (required — injects the GPUs/driver),
**`--shm-size=64g`** (Spark/RAPIDS shuffle + optional `STAGE_RAMDISK`; raise
toward dataset size if staging to `/dev/shm`), **`-v "$PWD/data:/data"`**
(persists data + results on the host), **`-e SCALE=`/`-e SF=`** (scale factors).

### Compose

```bash
cd docker-all
SCALE=100 docker compose run --rm bench tpch-all
SF=10     docker compose run --rm bench ssb-all
```

## Commands (entrypoint)

| Command | What |
|---------|------|
| `tpch-gen` | generate SF`$SCALE` parquet + 22-query stream |
| `tpch-spark` / `tpch-polars` | run TPC-H q1-22 on GPU (Spark-RAPIDS / cudf-polars) |
| `tpch-table` | merge both into the GPU comparison table |
| `tpch-all` / `tpch-smoke` | full TPC-H pipeline / SF1 self-test |
| `ssb-gen` | generate SSB SF`$SF` columnar data |
| `ssb-build` | compile Lancelot engine + minmax for `$SM_ARCH`/`$NUM_GPU`, run minmax |
| `ssb-run-gpu` / `ssb-run-cpu` | run the SSB engine on GPU / CPU |
| `ssb-all` / `ssb-smoke` | full SSB pipeline / SF1 self-test |
| `smoke` | both SF1 self-tests |
| `bash` | shell |

## Config (env)

| var | default | scope |
|-----|---------|-------|
| `SCALE` | 10 | TPC-H scale factor |
| `SF` | 1 | SSB scale — must be 1, 10, 20, or 40 |
| `NUM_GPU` | 2 | Lancelot GPU count |
| `SM_ARCH` | 120 | Lancelot CUDA arch (70/80/89/90/120) |
| `STAGE_RAMDISK` | 0 | stage TPC-H parquet into /dev/shm first (raise `--shm-size`) |
| `DRIVER_MEM` / `HOST_SPILL` | 96g / 200G | Spark-RAPIDS heap / host spill |

## Outputs (under `/data`)

```
data/sf<SCALE>/parquet, queries/, results/   # TPC-H data + result CSVs + table
data/ssb/s<SF>_columnar/                      # SSB columnar data (+ minmax)
data/logs/                                    # Lancelot run logs
```

## Caveats (validated)

- **TPC-H Spark-RAPIDS + Polars: fully working** on sm_120 (see `../tpch_sf500/`
  for SF500 results). Polars GPU auto-retries OOM queries on managed memory.
- **Lancelot builds + links for sm_120** (with the auto-applied fixes: dbgen `-f`,
  `--std=c++17`, libpapi-dev, `-x cu` minmax fallback) and generates data, but the
  **engine does not yet execute on Blackwell** (no sm_120 source path) — `minmax`
  segfaults / the engine exits non-zero. Try `SM_ARCH=90` (PTX-JIT) or a
  Volta/Ampere GPU. See `../lancelot/docker/README.md`.
- A single 32 GB GPU spills large TPC-H joins to host; CPU Polars is fastest at
  SF500 on this box (`../tpch_sf500/POLARS_RESULTS.md`).
