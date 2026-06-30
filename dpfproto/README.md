# GOLAP and DPFProto Baseline


## Paper notes
GOLAP: 
- uses GPU direct storage to stream data from SSD to GPU
- decompress on GPU during scan
- prune and query operators on GPU
- result: inflate effective SSD bandwith beyond raw SSD bandwidth
    - We should expect 4×-23× speedup over CPU SSD-based systems

DPFProto: 
- Data path fusion (DPF): exeutes a sequence of data path ops in 1 kernel (OP, decomp, database ops)
- Other GPU optimization techniques
    - type-specific compression/depression
    - variable length attribute support
    - BaM-based IO
    - works togther with DPF
Reported results:
- reimplements GOLAP baseline
- reports better TPC-H performance than GOLAP
- Main gains come from reducing GPU memory traffic and intermediate materialization

## Code running notes

### DPFProto
DPFProto reports results on a partial TPC-H suite: Q1, Q3, Q5, Q6, Q13, and Q16

This note tracks the minimal path for setting up DPFProto's TPC-H evaluation on Magnum

The BaM-backed modes require bare-metal access to NVMe devices

```bash
# 1st clone the official dpfproto repo
cd dpfproto
git clone --recursive https://github.com/dbc-utokyoiis/DPFProto.git
cd DPFProto

# setup checks
nvcc --version
cmake --version
gdscheck.py -p
lsmod | grep nvidia_fs

# provided by DPFProto to install nvidia compression libs
scripts/setup/install_nvidia_libs.sh

# shared local build env
export DPF_DEPS=$HOME/.local/dpfproto-deps
export CUDA_HOME=/usr/local/cuda-13.2
export CUDA_PATH=$CUDA_HOME
export PATH=$CUDA_HOME/bin:$CUDA_HOME/gds/tools:$DPF_DEPS/bin:$PATH
export PKG_CONFIG_PATH=$DPF_DEPS/lib/pkgconfig:$DPF_DEPS/lib64/pkgconfig:$PKG_CONFIG_PATH
export LD_LIBRARY_PATH=$DPF_DEPS/lib:$DPF_DEPS/lib64:$CUDA_HOME/lib64:$LD_LIBRARY_PATH
export LIBRARY_PATH=$DPF_DEPS/lib:$DPF_DEPS/lib64:$LIBRARY_PATH
export CMAKE_PREFIX_PATH=$DPF_DEPS:$CMAKE_PREFIX_PATH

# dependency fixups for Magnum
mkdir -p "$DPF_DEPS/src"
cd "$DPF_DEPS/src"
rm -f zlib-1.3.1.tar.gz
rm -rf zlib-1.3.1
curl -L -o zlib-1.3.1.tar.gz https://zlib.net/fossils/zlib-1.3.1.tar.gz
tar xf zlib-1.3.1.tar.gz
cd zlib-1.3.1
./configure --prefix="$DPF_DEPS"
make -j$(nproc)
make install
cd ..

# ====== config issue fixing ==========
# add missing snappy pkg-config file
mkdir -p "$DPF_DEPS/lib/pkgconfig"
cat > "$DPF_DEPS/lib/pkgconfig/snappy.pc" <<EOF
prefix=$DPF_DEPS
exec_prefix=\${prefix}
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: snappy
Description: Fast compression/decompression library
Version: 1.2.2
Libs: -L\${libdir} -lsnappy
Cflags: -I\${includedir}
EOF

# check deps
pkg-config --modversion liblz4 zlib snappy
# ================

# build DPFProto
cd ~/work/TQP-Vortex-Baselines/dpfproto/DPFProto
git submodule update --init --recursive
mkdir -p build
cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CUDA_ARCHITECTURES=86
make -j$(nproc) tpchdb tpchloader
ls -lh tpchdb tpchloader

# prepare data -> data is put into DATA_BASE
# alter the following as needed
cd ~/work/TQP-Vortex-Baselines/dpfproto/DPFProto
export DATA_BASE=$HOME/work/TQP-Vortex-Baselines/dpfproto/data/tpch
mkdir -p "$DATA_BASE"


# build official TPC-H dbgen
# this was downloaded from web and placed into the dir below
cd "/home/wangyuch/work/TPC-H V3.0.1/dbgen"
cp makefile.suite Makefile
sed -i 's/^CC[[:space:]]*=.*/CC      = gcc/' Makefile
sed -i 's/^DATABASE[[:space:]]*=.*/DATABASE= INFORMIX/' Makefile
sed -i 's/^MACHINE[[:space:]]*=.*/MACHINE = LINUX/' Makefile
sed -i 's/^WORKLOAD[[:space:]]*=.*/WORKLOAD= TPCH/' Makefile
make clean 2>/dev/null || true
make -j$(nproc)
./dbgen -h

# generate raw TPC-H data
export TPCH_DBGEN_DIR="/home/wangyuch/work/TPC-H V3.0.1/dbgen"
cd ~/work/TQP-Vortex-Baselines/dpfproto/DPFProto

# DPFProto's dbgen wrapper does not quote dbgen paths, so patch a local copy.
cp scripts/tpch/dbgen.sh scripts/tpch/dbgen.local.sh
sed -i 's|$dbgendir/dbgen|"$dbgendir/dbgen"|g' scripts/tpch/dbgen.local.sh

cp scripts/tpch/run_dbgen.sh scripts/tpch/run_dbgen.local.sh
sed -i 's|bash "${SCRIPT_DIR}/dbgen.sh"|bash "${SCRIPT_DIR}/dbgen.local.sh"|g' \
  scripts/tpch/run_dbgen.local.sh

rm -rf "$DATA_BASE/input1"
scripts/tpch/run_dbgen.local.sh 1 $(nproc) "$DATA_BASE"
find "$DATA_BASE/input1" -maxdepth 2 -type f | head -20

# prepare sideways/GOLAP-style data
# DPFProto's sideways script writes to /export by default, so patch a local copy.
cp scripts/golap/01_sideways_pruning.sh scripts/golap/01_sideways_pruning.local.sh
sed -i 's|^INPUT_BASE_DIR="/export/data1/tpch/input${SF}"|DATA_BASE="${DATA_BASE:-/export/data1/tpch}"\
INPUT_BASE_DIR="${DATA_BASE}/input${SF}"|' scripts/golap/01_sideways_pruning.local.sh
sed -i 's|^OUTPUT_BASE_DIR="/export/data1/tpch/sideways/sf${SF}"|OUTPUT_BASE_DIR="${DATA_BASE}/sideways/sf${SF}"|' scripts/golap/01_sideways_pruning.local.sh
DATA_BASE="$DATA_BASE" bash scripts/golap/01_sideways_pruning.local.sh -s 1 -n $(nproc)

# run full non-BaM suite
scripts/tpch_run_all.sh \
  -s 1 \
  -q q1,q3,q5,q6,q13,q16 \
  -n 3 \
  --skip gidp+bam \
  --skip gidp+bam+fusion \
  --skip datapathfusion \
  --no-revenue

# run BaM suite (requires sudo / bare-metal NVMe access)
sudo scripts/tpch_run_all.sh \
  -s 1 \
  -q q1,q3,q5,q6,q13,q16 \
  -n 3 \
  --skip gidp \
  --no-revenue
  
# outputs:
# logs/tpch_run_all/<timestamp>/sf<SF>/<mode>/<query>.txt
```

### GOLAP (baseline in DPFProto codebase)

Running GOLAP end-to-end

```bash
# From DPFProto root
SF=1
THREADS=$(nproc)
TRIALS=1
QUERIES="q1 q3 q5 q6 q13 q16"

# Device list from scripts/common.sh.
# Confirm these are safe on Magnum before using them.
source scripts/common.sh
export DATA_BASE=$HOME/work/TQP-Vortex-Baselines/dpfproto/data/tpch

# 1. Prepare GOLAP-style sideways data.
# No sudo needed: this only reads/writes files under DATA_BASE.
DATA_BASE="$DATA_BASE" bash scripts/golap/01_sideways_pruning.local.sh -s ${SF} -n ${THREADS}

# 2. Load GOLAP-style compressed layout.
# Sudo needed: this writes DPFProto pages to DEVICES_NVME.
sudo env CUFILE_ENV_PATH_JSON="${CUFILE_ENV_PATH_JSON}" \
  ./build/tpchloader \
  -i "${DATA_BASE}/sideways/sf${SF}" \
  -d "${DEVICES_NVME}" \
  -x gidp \
  -A \
  -c

# 3. Run the filesystem/GDS GOLAP-style baseline.
# Sudo likely needed: this reads DEVICES_NVME through the GDS path.
mkdir -p logs/golap_gidp_sf${SF}
for q in ${QUERIES}; do
  scripts/bench.sh -n ${TRIALS} -w -t 30 \
    -o logs/golap_gidp_sf${SF}/${q}.txt \
    -- sudo env CUFILE_ENV_PATH_JSON="${CUFILE_ENV_PATH_JSON}" \
      ./build/tpchdb \
      -q "${q}" \
      -x gidp \
      -w ${THREADS} \
      -Z \
      "${DEVICES_NVME}"
done

# 4. Optional BaM baseline.
# Sudo definitely needed: this can unmount/rebind NVMe devices.
mkdir -p logs/golap_bam_sf${SF}
sudo scripts/tpch_run_all.sh \
  -s ${SF} \
  -q q1,q3,q5,q6,q13,q16 \
  -n ${TRIALS} \
  --skip gidp \
  --skip gidp+bam+fusion \
  --skip datapathfusion \
  --no-load \
  --no-revenue
```
