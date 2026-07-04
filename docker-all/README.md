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

Every flag used above, explained:

| flag | meaning |
|------|---------|
| `docker build -t tpch-multitool .` | `-t tpch-multitool` tags (names) the image; `.` is the build context (this `docker-all/` dir). |
| `docker run` | create + start a container from the image. |
| `--rm` | auto-delete the container when it exits. Results are safe — they live on the mounted volume, not in the container. |
| `-it` | `-i` (keep stdin open) + `-t` (allocate a TTY) so progress/logs stream live to your terminal. Omit both for unattended/CI runs. |
| `--gpus all` | **required.** Exposes the host GPUs and injects the NVIDIA driver into the container. Use `--gpus '"device=0"'` to pin a single GPU. |
| `--shm-size=64g` | size of `/dev/shm`. **TPC-H runs stage parquet to ramdisk by default**, so set this **≥ the dataset** (~`SCALE`×0.36 GB; SF100 ≈ 36 GB, SF500 ≈ 180 GB) — otherwise the run prints a notice and reads from disk instead. Also backs Spark/RAPIDS shuffle; Docker's 64 MB default is far too small regardless. |
| `-v "$PWD/data:/data"` | bind-mount host `./data` → container `/data`, so generated data **and** results persist on the host after `--rm`. |
| `-e SCALE=100` | env var: TPC-H scale factor in GB, used by the `tpch-*` commands. |
| `-e SF=10` | env var: SSB scale factor for the `ssb-*` commands — must be `1`, `10`, `20`, or `40`. |
| `-e NUM_GPU=2` | env var: number of GPUs Lancelot partitions across (`ssb-build`). |
| `-e SM_ARCH=120` | env var: CUDA arch Lancelot compiles for — `120` Blackwell/RTX 5090, `90` Hopper, `89` Ada, `80` Ampere, `70` Volta. |
| `tpch-multitool` | the image name to run (must come after all `docker run` options). |
| trailing `tpch-all` / `ssb-all` / … | the entrypoint command to execute (see the table below). |

Compose: `docker compose run --rm bench tpch-all` — `run` starts a one-off
container of the `bench` service, `--rm` removes it on exit, and `tpch-all`
overrides the service's default command. `--gpus`, `--shm-size`, the volume, and
env vars all come from `docker-compose.yml`.

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
| `STAGE_RAMDISK` | **1** | stage TPC-H parquet into /dev/shm (ramdisk) before running — **on by default**; needs `--shm-size` ≥ dataset (~`SCALE`×0.36 GB), else auto-falls back to disk. Set `0` to force disk. |
| `DRIVER_MEM` / `HOST_SPILL` | 96g / 200G | Spark-RAPIDS heap / host spill |

## Outputs (under `/data`)

```
data/sf<SCALE>/parquet, queries/, results/   # TPC-H data + result CSVs + table
data/ssb/s<SF>_columnar/                      # SSB columnar data (+ minmax)
data/logs/                                    # Lancelot run logs
```

## Caveats (validated)

- **TPC-H Spark-RAPIDS + Polars: fully working** on sm_120 (see `../results/`
  for SF500 results). Polars GPU auto-retries OOM queries on managed memory.
- **Lancelot builds + links for sm_120** (with the auto-applied fixes: dbgen `-f`,
  `--std=c++17`, libpapi-dev, `-x cu` minmax fallback) and generates data, but the
  **engine does not yet execute on Blackwell** (no sm_120 source path) — `minmax`
  segfaults / the engine exits non-zero. Try `SM_ARCH=90` (PTX-JIT) or a
  Volta/Ampere GPU. See `../lancelot/docker/README.md`.
- A single 32 GB GPU spills large TPC-H joins to host; CPU Polars is fastest at
  SF500 on this box (`../results/POLARS_RESULTS.md`).
