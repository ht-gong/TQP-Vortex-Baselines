#!/bin/bash
# Run every engine's real runner (q1-q22) against TWO parquet datasets and check
# that each engine cannot tell them apart -- same status and same result row
# count per query -- and that all engines agree with each other. Used to certify
# that data from datagen/gen_tpch.sh is interchangeable with the upstream NDS-H
# pipeline's from the engines' point of view.
#
#   datagen/verify_engines.sh <SF> <parquet_A> <parquet_B> <out_dir>
#
# Env: ENGINES (default "duckdb_cpu polars_gpu rapids sirius"; also polars_cpu),
#      QUERIES (default 1..22), LABEL_A/LABEL_B (names in the report; default
#      upstream/datagen), plus the runners' own knobs (DRIVER_MEM, MIN_FREE_GB...).
# Runs with TPCH_MERGE=0, so nothing is folded into results/all_results.csv; the
# per-run CSVs are kept in <out_dir> and compared by compare_engine_runs.py.
set -uo pipefail

SF="${1:?sf}"; PQ_A="${2:?parquet_A}"; PQ_B="${3:?parquet_B}"; OUT="${4:?out_dir}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
ENGINES="${ENGINES:-duckdb_cpu polars_gpu rapids sirius}"
QUERIES="${QUERIES:-$(seq 1 22)}"
LABEL_A="${LABEL_A:-upstream}"; LABEL_B="${LABEL_B:-datagen}"
export TPCH_MERGE=0 TPCH_SF="${SF}"
mkdir -p "${OUT}"
LOG="${OUT}/verify_engines.log"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }

run_engine(){   # $1=engine $2=parquet $3=out_csv
  local e="$1" pq="$2" csv="$3"
  case "${e}" in
    rapids)     TPCH_PARQUET="${pq}" OUT_CSV="${csv}" bash "${ROOT}/rapids/run_tpch_safe.sh"  "${QUERIES}" ;;
    polars_gpu) TPCH_PARQUET="${pq}" OUT_CSV="${csv}" bash "${ROOT}/polars/run_polars_gpu.sh" "${QUERIES}" ;;
    polars_cpu) TPCH_PARQUET="${pq}" OUT_CSV="${csv}" bash "${ROOT}/polars/run_polars.sh"     "${QUERIES}" ;;
    duckdb_cpu) TPCH_PARQUET="${pq}" OUT_CSV="${csv}" bash "${ROOT}/duckdb/run_duckdb.sh"      "${QUERIES}" ;;
    sirius)     SIRIUS_PARQUET="${pq}" OUT_CSV="${csv}" bash "${ROOT}/sirius/run_sirius.sh"    "${QUERIES}" ;;
    *) log "unknown engine '${e}'"; return 1 ;;
  esac
}

log "engine verification SF${SF}: engines=[${ENGINES}] queries=[$(echo ${QUERIES})]"
log "  ${LABEL_A}: ${PQ_A}"
log "  ${LABEL_B}: ${PQ_B}"
[ -n "${CUDA_VISIBLE_DEVICES:-}" ] && log "  CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES}"

# As run.sh does when staging: drop 0-row parquet part-files (Spark writes one
# per table whose partition 0 was empty; Sirius' GPU reader rejects them).
# Applied identically to both datasets, in place.
for pq in "${PQ_A}" "${PQ_B}"; do
  n=$("${ROOT}/datagen-venv/bin/python" - "${pq}" <<'EOF'
import glob, os, sys, pyarrow.parquet as pq
n = 0
for f in glob.glob(os.path.join(sys.argv[1], "*", "*.parquet")):
    if pq.ParquetFile(f).metadata.num_rows == 0:
        os.remove(f); n += 1
print(n)
EOF
)
  log "  dropped ${n} empty part-file(s) from ${pq}"
done
for e in ${ENGINES}; do
  for lab in A B; do
    if [ "${lab}" = A ]; then pq="${PQ_A}"; name="${LABEL_A}"; else pq="${PQ_B}"; name="${LABEL_B}"; fi
    csv="${OUT}/${e}__${name}.csv"
    log "--- ${e} on ${name} ---"
    t0=$(date +%s)
    run_engine "${e}" "${pq}" "${csv}" >"${OUT}/${e}__${name}.log" 2>&1 || log "    ${e} on ${name}: runner exited nonzero"
    if [ -s "${csv}" ]; then
      log "    $(grep -c ',OK,' "${csv}")/$(( $(wc -l < "${csv}") - 1 )) OK in $(( $(date +%s) - t0 ))s -> ${csv}"
    else
      log "    !! no CSV produced (see ${OUT}/${e}__${name}.log)"
    fi
  done
done

log "comparing ..."
"${ROOT}/datagen-venv/bin/python" "${HERE}/compare_engine_runs.py" "${OUT}" \
  --label-a "${LABEL_A}" --label-b "${LABEL_B}" --report "${OUT}/engine_report.md" | tee -a "${LOG}"
