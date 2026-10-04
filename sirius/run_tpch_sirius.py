#!/usr/bin/env python
"""Run TPC-H queries 1-22 through Sirius (GPU) on the SF500 parquet via DuckDB.

Sirius (github.com/sirius-db/sirius) is a GPU-native SQL engine that loads as a
DuckDB extension and transparently intercepts plain SQL, running supported
operators on the GPU (cuDF/RMM/cuCascade) with out-of-core tiered spilling
(GPU -> pinned host -> disk). We drive its bundled `duckdb` binary, which has
the extension statically linked and auto-loading, via `-f <file.sql>`.

Mirrors the rapids/polars runners so the baselines are comparable:
  * reads the SAME marker-delimited query stream the rapids run used
    (results/queries/stream_qualification.sql), so the SQL is identical;
  * the dataset is read from the ramdisk parquet (/dev/shm) as DuckDB views;
  * protocol (as every engine, see AGENTS.md; run_sirius.sh starts one process
    per query): an untimed warm pass first runs the query on the SF1 dataset in
    $WARM_PARQUET (GPU init, cuDF kernel JIT, reader setup; its output is
    discarded), then the views point at the target dataset and the query runs
    once, timed by the CLI's `.timer on` (engine startup excluded). An error
    anywhere in the process, the warm pass included, fails the query;
  * results are written incrementally to a CSV so a watchdog kill never loses
    prior rows. A companion *_detail.csv adds the GPU/fallback flag.

  run_tpch_sirius.py <parquet_dir> <stream.sql> <out_csv> [SUBSET] [append]
    SUBSET  comma-separated query numbers, e.g. "9" or "1,2,3"  (default 1..22)

Environment:
  SIRIUS_DUCKDB        path to the built duckdb binary
  SIRIUS_ENVLIB        its pixi env's lib dir (libcudf, rmm, cudart, ...); put on
                       the binary's LD_LIBRARY_PATH (its RPATH names it as well)
  SIRIUS_CONFIG_FILE   path to the gpu_execution YAML config (required by Sirius)
  WARM_PARQUET         the SF1 dataset of the warm pass
  SIRIUS_TIMEOUT       per-query subprocess timeout in seconds (default 1800)
  SIRIUS_LOG_DIR       if set, Sirius writes its spdlog file here; we grep it to
                       tell whether the query actually ran on GPU or fell back.
"""
import os
import re
import sys
import time
import glob
import subprocess
from collections import OrderedDict

HERE = os.path.dirname(os.path.abspath(__file__))
PARQUET = sys.argv[1].rstrip("/")
STREAM = sys.argv[2]
OUT_CSV = sys.argv[3]
SUBSET = [int(x) for x in sys.argv[4].split(",")] if len(sys.argv) > 4 and sys.argv[4] else list(range(1, 23))
APPEND = len(sys.argv) > 5 and sys.argv[5] == "append"

DUCKDB = os.environ["SIRIUS_DUCKDB"]
WARM = (os.environ.get("WARM_PARQUET") or sys.exit(
    "WARM_PARQUET is not set (the SF1 dataset of the warm pass)")).rstrip("/")
TIMEOUT = int(os.environ.get("SIRIUS_TIMEOUT", "1800"))
LOG_DIR = os.environ.get("SIRIUS_LOG_DIR", "")
DETAIL_CSV = os.environ.get("SIRIUS_DETAIL_CSV", os.path.splitext(OUT_CSV)[0] + "_detail.csv")

TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]

RUN_MARK = "__SIRIUS_TIMED_RUN__"
RUN_TIME_RE = re.compile(r"Run Time \(s\): real ([0-9]+\.[0-9]+)")
# stderr noise that must not be miscounted as CSV result rows. `mbind: Operation
# not permitted` is emitted whenever Sirius grows a NUMA-pinned host pool in a
# container without CAP_SYS_NICE (docker/run.sh grants it; `make doctor` checks).
NOISE_RE = re.compile(r"mbind:|Operation not permitted|terminate called|what\(\):|^\[[0-9]{4}-|^\s*$")


def parse_stream(path):
    """{query_num(int): [sql_statements]} -- identical parsing to the rapids
    runner: split each template body on ';', keep fragments that contain real
    SQL (so Q15 becomes create-view / select / drop-view)."""
    with open(path) as f:
        stream = f.read()
    pat = re.compile(r'-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)', re.DOTALL)
    out = OrderedDict()
    for num, body in pat.findall(stream):
        stmts = [s.strip() for s in body.split(";")
                 if re.search(r'\b(select|create|drop|with)\b', s, re.I)]
        out[int(num)] = stmts
    return out


def view_sql(root):
    """CREATE OR REPLACE VIEW over the ramdisk parquet under root (one sub-dir of
    part-*.parquet per table, as produced by the NDS-H transcode pipeline)."""
    lines = []
    for t in TABLES:
        files = sorted(glob.glob(f"{root}/{t}/*.parquet")) or [f"{root}/{t}.parquet"]
        lst = ", ".join(f"'{f}'" for f in files)
        lines.append(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet([{lst}]);")
    return "\n".join(lines)


def build_sql(stmts):
    query = ";\n".join(stmts) + ";"
    parts = [view_sql(WARM),
             # Warm pass: the same query on SF1, untimed (before `.timer on` and
             # the run marker), its result rows discarded.
             ".output /dev/null",
             query,
             ".output",
             view_sql(PARQUET),
             ".mode csv",
             ".headers off",
             ".timer on",
             f".print {RUN_MARK}",
             query]
    return "\n".join(parts) + "\n"


def parse_output(text):
    """The timed run, from the CLI's stdout: everything after the run marker.
    Returns (seconds, rows): the sum of its statements' 'Run Time' values (total
    wall time) and its CSV result rows (non-timer lines); None without a marker
    (the CLI stops at the first error, so a failed warm pass leaves none)."""
    secs, rows, seen = 0.0, 0, False
    for line in text.splitlines():
        if line.startswith(RUN_MARK):
            seen = True
            continue
        if not seen:
            continue
        m = RUN_TIME_RE.search(line)
        if m:
            secs += float(m.group(1))
        elif line.strip() and not NOISE_RE.search(line):
            rows += 1                     # a CSV result row of the timed run
    return (secs, rows) if seen else None


def newest_log():
    logs = sorted(glob.glob(os.path.join(LOG_DIR, "sirius*.log")), key=os.path.getmtime) if LOG_DIR else []
    return logs[-1] if logs else None


def gpu_or_fallback(log_before):
    """Best-effort: inspect the newest Sirius log written during this run to see
    whether Sirius handled the query on GPU or DuckDB CPU fallback kicked in.
    log_before = (path, size) of the newest log before the run; Sirius starts a
    new (dated) file each day, and then the whole new file belongs to this run."""
    log = newest_log()
    if not log:
        return "?"
    start = log_before[1] if log == log_before[0] else 0
    try:
        with open(log, errors="ignore") as f:
            txt = f.read()[start:]
    except OSError:
        return "?"
    low = txt.lower()
    # A real CPU fallback shows up as a scan/plan error that Sirius drains and
    # hands back to DuckDB. Do NOT match the substring "fallback" alone -- the
    # init line "pinned memory resource configured (... fallback node=0)" is
    # NUMA config noise, not an execution fallback.
    if "draining after error" in low or "error executing query" in low:
        return "fallback"
    # Count GPU vs total query completions in this segment. Transparent GPU runs
    # log "Transparent GPU execution: query completed" per GPU-executed statement.
    gpu_done = low.count("transparent gpu execution: query completed")
    if gpu_done > 0:
        return "gpu"
    return "?"


def run_query(num, stmts):
    sql = build_sql(stmts)
    tmp = f"/tmp/sirius_q{num}.sql"
    with open(tmp, "w") as f:
        f.write(sql)
    log = newest_log()
    log_before = (log, os.path.getsize(log) if log else 0)
    env = dict(os.environ)
    if os.environ.get("SIRIUS_ENVLIB"):
        env["LD_LIBRARY_PATH"] = os.environ["SIRIUS_ENVLIB"]
    t0 = time.time()
    try:
        p = subprocess.run([DUCKDB, "-f", tmp], capture_output=True, text=True,
                           timeout=TIMEOUT, env=env)
        out = p.stdout + "\n" + p.stderr
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") + "\n" + (e.stderr or "") if isinstance(e.stdout, str) else ""
        return dict(status="TIMEOUT", secs=time.time() - t0, rows="killed_timeout", gpu="?")
    run = parse_output(p.stdout)
    err = None
    for pat in ("Error:", "Invalid Error", "IO Error", "Catalog Error", "Parser Error",
                "Binder Error", "Out of Memory", "std::bad_alloc", "CUDA", "RMM error"):
        m = re.search(rf"^.*{re.escape(pat)}.*$", out, re.MULTILINE)
        if m:
            err = m.group(0).strip()[:120].replace(",", ";")
            break
    if run is None:
        return dict(status="FAIL", secs=time.time() - t0, rows=(err or "no_output")[:120], gpu="?")
    secs, rows = run
    if err and (p.returncode != 0):
        return dict(status="FAIL", secs=secs, rows=err, gpu="?")
    return dict(status="OK", secs=secs, rows=rows, gpu=gpu_or_fallback(log_before))

def main():
    print(f"Sirius run | duckdb={DUCKDB}")
    print(f"config={os.environ.get('SIRIUS_CONFIG_FILE','<none>')} | warm={WARM} | timeout={TIMEOUT}s")
    if not os.path.exists(DUCKDB):
        sys.exit(f"ERROR: duckdb binary not found at {DUCKDB} (build Sirius first)")
    queries = parse_stream(STREAM)

    write_header = not (APPEND and os.path.exists(OUT_CSV))
    f = open(OUT_CSV, "a" if APPEND else "w")
    fd = open(DETAIL_CSV, "a" if (APPEND and os.path.exists(DETAIL_CSV)) else "w")
    if write_header:
        f.write("query,status,seconds,result_rows_or_error\n"); f.flush()
        fd.write("query,status,seconds,rows,gpu\n"); fd.flush()

    print(f"\n{'query':10} {'status':8} {'secs':>9} {'rows':>9}  gpu")
    ok = 0
    for n in SUBSET:
        if n not in queries:
            continue
        name = f"query{n}"
        r = run_query(n, queries[n])
        print(f"{name:10} {r['status']:8} {r['secs']:9.2f} {str(r['rows']):>9}  {r['gpu']}")
        f.write(f"{name},{r['status']},{r['secs']:.3f},{r['rows']}\n"); f.flush()
        fd.write(f"{name},{r['status']},{r['secs']:.3f},{r['rows']},{r['gpu']}\n"); fd.flush()
        if r["status"] == "OK":
            ok += 1
    f.close(); fd.close()
    print(f"\n{ok}/{len([n for n in SUBSET if n in queries])} queries OK | csv -> {OUT_CSV}")
    sys.exit(0 if ok == len([n for n in SUBSET if n in queries]) else 1)

if __name__ == "__main__":
    main()
