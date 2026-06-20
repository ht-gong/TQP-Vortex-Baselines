#!/bin/bash
# Generate the SSB dataset at scale factor $SF and load it into Lancelot's
# columnar binary layout under $BASE_PATH/s<SF>_columnar/ (what the engine reads).
#   gen_data.sh [SF]
set -euo pipefail
SF="${1:-${SF:-1}}"
BASE_PATH="${BASE_PATH:-/data/ssb/}"; BASE_PATH="${BASE_PATH%/}/"
RAW="${BASE_PATH}s${SF}_raw"
COL="${BASE_PATH}s${SF}_columnar"
ROOT="${LANCELOT_ROOT:-/opt/lancelot}"
log(){ echo "[lancelot:gen] $*"; }

mkdir -p "${RAW}" "${COL}"

log "dbgen -s ${SF} -T a -f"
# -f forces overwrite: without it, dbgen prompts "overwrite ?" and, with no stdin
# (background/Docker), spins forever on that prompt.
( cd "${ROOT}/test/ssb/dbgen" && rm -f ./*.tbl && ./dbgen -s "${SF}" -T a -f && mv ./*.tbl "${RAW}/" )

log "convert .tbl -> .tbl.p"
( cd "${ROOT}/test/ssb/loader" && python3 convert.py "${RAW}/" )

log "load -> columnar (${COL})"
( cd "${ROOT}/test/ssb/loader" && ./loader \
    --lineorder "${RAW}/lineorder.tbl" \
    --ddate     "${RAW}/date.tbl" \
    --customer  "${RAW}/customer.tbl.p" \
    --supplier  "${RAW}/supplier.tbl.p" \
    --part      "${RAW}/part.tbl.p" \
    --datadir   "${COL}/" )

log "DONE. columnar: $(du -sh "${COL}" | cut -f1)  (raw kept at ${RAW})"
