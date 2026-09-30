#!/bin/bash
# Generate the parquet-format ablation variants of one scale factor from ONE
# dbgen run (results/FORMAT_ABLATION.md). Each variant is the same rows,
# transcoded by the same Spark/parquet-mr pipeline with different writer knobs.
#
#   datagen/gen_format_variants.sh <SF> <PARALLEL> <BATCH> <out_root> [variant ...]
#
# Output: <out_root>/<variant>/parquet/<table>/ plus <out_root>/_rawstore/ (the
# shared dbgen text; delete when done). Variant = <layout>-<encoding>-<codec>:
#   layout   shuffle | keyorder      encoding  plain | dict | delta
#   codec    snappy | zstd | lz4raw
# Default variant list = the 12 the ablation uses. Existing variants are skipped.
# Prints one TIMING line per variant (dbgen time counted only where it ran).
set -euo pipefail
SF="${1:?sf}"; PAR="${2:?parallel}"; BATCH="${3:?batch}"; OUT="${4:?out_root}"; shift 4
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VARIANTS=("$@")
[ ${#VARIANTS[@]} -gt 0 ] || VARIANTS=(shuffle-dict-snappy shuffle-plain-snappy shuffle-delta-snappy \
  shuffle-dict-zstd shuffle-plain-zstd shuffle-delta-zstd \
  shuffle-dict-lz4raw shuffle-plain-lz4raw shuffle-delta-lz4raw \
  keyorder-dict-snappy keyorder-plain-snappy keyorder-delta-snappy)

opts_for(){   # variant -> TRANSCODE_OPTS
  local layout enc codec; IFS=- read -r layout enc codec <<<"$1"
  local o="--layout ${layout} --compression ${codec}"
  case "${enc}" in
    dict)  o+=" --dictionary true --writer-version v1" ;;
    plain) o+=" --dictionary false --writer-version v1" ;;
    delta) o+=" --dictionary false --writer-version v2" ;;
    *) echo "bad encoding '${enc}' in variant $1" >&2; return 1 ;;
  esac
  echo "${o}"
}

mkdir -p "${OUT}"
for v in "${VARIANTS[@]}"; do
  if [ -d "${OUT}/${v}/parquet" ]; then echo "[$(date +%T)] ${v}: exists, skipping"; continue; fi
  opts="$(opts_for "${v}")"
  echo "[$(date +%T)] ${v}: ${opts}"
  RAW_STORE="${OUT}/_rawstore" TRANSCODE_OPTS="${opts}" \
    "${HERE}/gen_tpch.sh" "${SF}" "${PAR}" "${BATCH}" "${OUT}/${v}" | grep -E 'TIMING|VALID|FAIL|reusing raw' | sed "s/^/  ${v}: /"
done
echo "[$(date +%T)] all variants done under ${OUT}; shared raw text in ${OUT}/_rawstore ($(du -sh "${OUT}/_rawstore" | cut -f1))"
