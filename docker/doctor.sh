#!/bin/bash
# `make doctor`: is this container ready to produce comparable numbers?
# Checks the installed versions against the pins, the GPU, io_uring (Sirius
# silently falls back to kvikio without it) and free space. Exits non-zero if a
# check fails.
set -uo pipefail
PINS=/opt/envs/pins
fail=0
ok(){ printf '  %-10s ok    %s\n' "$1" "$2"; }
bad(){ printf '  %-10s FAIL  %s\n' "$1" "$2"; fail=1; }
warn(){ printf '  %-10s warn  %s\n' "$1" "$2"; }
pin(){ sed -n "s/^$1=//p" "${PINS}/versions.env"; }

echo "versions"
for env in py polars; do
  py="$([ "${env}" = py ] && echo "${PY}" || echo "${POLARS_PY}")"
  want="$(grep -E '^[A-Za-z0-9_.-]+==' "${PINS}/${env}.lock" | sed 's/ .*//' | tr 'A-Z_' 'a-z-' | sort)"
  have="$("${py}" -m pip freeze 2>/dev/null | tr 'A-Z_' 'a-z-' | sort)"
  if [ "${want}" = "${have}" ]; then ok "${env}" "$(echo "${want}" | wc -l) packages match ${env}.lock"
  else bad "${env}" "differs from ${env}.lock: $(diff <(echo "${want}") <(echo "${have}") | grep '^[<>]' | tr '\n' ' ')"; fi
done
jv="$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)"
[[ "${jv}" == *17.0.20.1* ]] && ok java "${jv}" || bad java "${jv}"
sha="$(sha1sum "${RAPIDS_JAR}" | cut -c1-40)"
[ "${sha}" = "$(pin RAPIDS_JAR_SHA1)" ] && ok rapids "$(basename "${RAPIDS_JAR}")" || bad rapids "jar sha1 ${sha}"
sv="$(SIRIUS_DISABLE=1 "${SIRIUS_DUCKDB}" -noheader -list -c 'select version()' 2>/dev/null | grep -v -e mbind -e '^$' | tail -1)"
[ -n "${sv}" ] && ok sirius "duckdb ${sv}, sirius $(pin SIRIUS_REF | cut -c1-8)" || bad sirius "binary does not start"
[ -x "${DBGEN_DIR}/dbgen" ] && ok dbgen "$("${DBGEN_DIR}/dbgen" -h 2>&1 | head -1 | tr -d '\r')" || bad dbgen "missing"

echo "gpu"
if ! command -v nvidia-smi >/dev/null; then bad gpu "no GPU in the container (set GPU in local.env)"
else
  while IFS=, read -r idx name mode util mem; do
    [ "$(echo ${mode})" = Default ] || bad gpu "${name} is in ${mode} compute mode (Sirius fails at startup)"
    # an idle H100 shows a few MiB used by the driver
    if [ "${util// /}" != 0 ] || [ "${mem// /}" -gt 64 ]; then
      warn gpu "${name}: ${util// /}% busy, ${mem// /} MiB used by others -> timing noise"
    else ok gpu "${name} idle, $(echo ${mode}) mode"; fi
  done < <(nvidia-smi --query-gpu=index,name,compute_mode,utilization.gpu,memory.used \
             --format=csv,noheader,nounits)
fi

echo "io"
if python3 - <<'PY'
import ctypes, sys
libc = ctypes.CDLL(None, use_errno=True)
sys.exit(0 if libc.syscall(425, 8, ctypes.create_string_buffer(120)) >= 0 else 1)   # io_uring_setup
PY
then ok io_uring "allowed (Sirius uses its native io_uring datasource)"
else bad io_uring "blocked: Sirius would fall back to kvikio (check the seccomp profile)"; fi
[ "$(ulimit -l)" = unlimited ] && ok memlock unlimited || bad memlock "$(ulimit -l) KiB"

echo "space"
for d in / "${DATA_DIR}" "${SCRATCH}" "${SHM:-/dev/shm}"; do
  printf '  %-10s %s free  (%s)\n' "" "$(df -h --output=avail "${d}" | tail -1 | tr -d ' ')" "${d}"
done
exit "${fail}"
