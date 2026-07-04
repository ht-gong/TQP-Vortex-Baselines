#!/usr/bin/env python
"""Run TPC-H queries 1-22 through Sirius (GPU) on the SF500 parquet via DuckDB.

Sirius (github.com/sirius-db/sirius) is a GPU-native SQL engine that loads as a
DuckDB extension and transparently intercepts plain SQL, running supported
operators on the GPU (cuDF/RMM/cuCascade) with out-of-core tiered spilling
(GPU -> pinned host -> disk). We drive its bundled `duckdb` binary, which has
the extension statically linked and auto-loading, via `-f <file.sql>`.

Mirrors the rapids/polars runners so the three baselines are comparable:
  * reads the SAME marker-delimited query stream the rapids run used
    (results/queries/stream_qualification.sql), so the SQL is identical;
  * per-query wall time excludes engine startup -- a tiny warm-up query absorbs
    the one-time GPU/cuDF kernel JIT so it is not charged to any query;
  * the dataset is read from the ramdisk parquet (/dev/shm) as DuckDB views;
  * results are written incrementally to a CSV so a watchdog kill never loses
    prior rows.

Per query we run ITERS iterations in one process: iteration 0 is the cold-scan
time (comparable to the rapids/polars cold parquet scan), later iterations are
warm (Sirius scan cache). The primary CSV records the cold time in `seconds`;
a companion *_detail.csv records cold, warm(best) and the GPU/fallback flag.

  run_tpch_sirius.py <parquet_dir> <stream.sql> <out_csv> [SUBSET] [append]
    SUBSET  comma-separated query numbers, e.g. "9" or "1,2,3"  (default 1..22)

Environment:
  SIRIUS_DUCKDB        path to the built duckdb binary (default: sirius/sirius/build/release/duckdb)
  SIRIUS_CONFIG_FILE   path to the gpu_execution YAML config (required by Sirius)
  SIRIUS_ITERS         iterations per query (default 2: 1 cold + 1 warm)
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

DUCKDB = os.environ.get("SIRIUS_DUCKDB", os.path.join(HERE, "sirius", "build", "release", "duckdb"))
ITERS = int(os.environ.get("SIRIUS_ITERS", "2"))
TIMEOUT = int(os.environ.get("SIRIUS_TIMEOUT", "1800"))
LOG_DIR = os.environ.get("SIRIUS_LOG_DIR", "")
DETAIL_CSV = os.environ.get("SIRIUS_DETAIL_CSV", os.path.splitext(OUT_CSV)[0] + "_detail.csv")

TABLES = ["customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier"]

ITER_MARK = "__SIRIUS_ITER__"
RUN_TIME_RE = re.compile(r"Run Time \(s\): real ([0-9]+\.[0-9]+)")
# stderr noise that must not be miscounted as CSV result rows. `mbind: Operation
# not permitted` is emitted whenever Sirius grows a NUMA-pinned host pool (this
# container blocks the mbind syscall) -- harmless, but it can appear mid-query.
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


def view_sql():
    """CREATE VIEW over the ramdisk parquet (one sub-dir of part-*.parquet per
    table, as produced by the NDS-H transcode pipeline)."""
    lines = []
    for t in TABLES:
        files = sorted(glob.glob(f"{PARQUET}/{t}/*.parquet")) or [f"{PARQUET}/{t}.parquet"]
        lst = ", ".join(f"'{f}'" for f in files)
        lines.append(f"CREATE VIEW {t} AS SELECT * FROM read_parquet([{lst}]);")
    return "\n".join(lines)


def build_sql(stmts):
    per_iter = ";\n".join(stmts) + ";"
    parts = [view_sql(),
             # Warm-up: exercise the GPU scan+aggregate path once so the cuDF /
             # kernel JIT is not charged to any measured query. Excluded (runs
             # before .timer on and before the first iteration marker).
             "SELECT n_regionkey, count(*) FROM nation GROUP BY n_regionkey;",
             ".mode csv",
             ".headers off",
             ".timer on"]
    for i in range(ITERS):
        parts.append(f".print {ITER_MARK}{i}")
        parts.append(per_iter)
    return "\n".join(parts) + "\n"


def parse_output(text):
    """Split combined stdout+stderr by iteration markers. For each iteration sum
    its per-statement 'Run Time' values (total wall time) and, for the last
    iteration, count the CSV result rows (non-timer, non-marker lines)."""
    iters = []          # list of (secs, rows)
    cur_secs, cur_rows, in_iter = 0.0, 0, False
    for line in text.splitlines():
        if line.startswith(ITER_MARK):
            if in_iter:
                iters.append((cur_secs, cur_rows))
            cur_secs, cur_rows, in_iter = 0.0, 0, True
            continue
        if not in_iter:
            continue
        m = RUN_TIME_RE.search(line)
        if m:
            cur_secs += float(m.group(1))
        elif line.strip() and not NOISE_RE.search(line):
            cur_rows += 1                     # a CSV result row of this iteration
    if in_iter:
        iters.append((cur_secs, cur_rows))
    return iters


def gpu_or_fallback(log_before):
    """Best-effort: inspect the newest Sirius log written during this run to see
    whether Sirius handled the query on GPU or DuckDB CPU fallback kicked in."""
    if not LOG_DIR:
        return "?"
    logs = sorted(glob.glob(os.path.join(LOG_DIR, "sirius*.log")), key=os.path.getmtime)
    if not logs:
        return "?"
    try:
        with open(logs[-1], errors="ignore") as f:
            txt = f.read()[log_before:]
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
    log_before = 0
    if LOG_DIR:
        logs = sorted(glob.glob(os.path.join(LOG_DIR, "sirius*.log")), key=os.path.getmtime)
        if logs:
            log_before = os.path.getsize(logs[-1])
    env = dict(os.environ)
    t0 = time.time()
    try:
        p = subprocess.run([DUCKDB, "-f", tmp], capture_output=True, text=True,
                           timeout=TIMEOUT, env=env)
        out = p.stdout + "\n" + p.stderr
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") + "\n" + (e.stderr or "") if isinstance(e.stdout, str) else ""
        return dict(status="TIMEOUT", cold=time.time() - t0, warm=None, rows="killed_timeout", gpu="?")
    iters = parse_output(out)
    err = None
    for pat in ("Error:", "Invalid Error", "IO Error", "Catalog Error", "Parser Error",
                "Binder Error", "Out of Memory", "std::bad_alloc", "CUDA", "RMM error"):
        m = re.search(rf"^.*{re.escape(pat)}.*$", out, re.MULTILINE)
        if m:
            err = m.group(0).strip()[:120].replace(",", ";")
            break
    if not iters:
        return dict(status="FAIL", cold=time.time() - t0, warm=None,
                    rows=(err or "no_output")[:120], gpu="?")
    cold = iters[0][0]
    rows = iters[-1][1]
    warm = min((s for s, _ in iters[1:]), default=None) if len(iters) > 1 else None
    if err and (p.returncode != 0):
        return dict(status="FAIL", cold=cold, warm=warm, rows=err, gpu="?")
    return dict(status="OK", cold=cold, warm=warm, rows=rows, gpu=gpu_or_fallback(log_before))


def main():
    print(f"Sirius run | duckdb={DUCKDB}")
    print(f"config={os.environ.get('SIRIUS_CONFIG_FILE','<none>')} | iters={ITERS} | timeout={TIMEOUT}s")
    if not os.path.exists(DUCKDB):
        sys.exit(f"ERROR: duckdb binary not found at {DUCKDB} (build Sirius first)")
    queries = parse_stream(STREAM)

    write_header = not (APPEND and os.path.exists(OUT_CSV))
    f = open(OUT_CSV, "a" if APPEND else "w")
    fd = open(DETAIL_CSV, "a" if (APPEND and os.path.exists(DETAIL_CSV)) else "w")
    if write_header:
        f.write("query,status,seconds,result_rows_or_error\n"); f.flush()
        fd.write("query,status,cold_s,warm_s,rows,gpu\n"); fd.flush()

    print(f"\n{'query':10} {'status':8} {'cold_s':>9} {'warm_s':>9} {'rows':>9}  gpu")
    ok = 0
    for n in SUBSET:
        if n not in queries:
            continue
        name = f"query{n}"
        r = run_query(n, queries[n])
        cold = r["cold"]
        warm_s = "" if r["warm"] is None else f"{r['warm']:.3f}"
        warm_disp = "-" if r["warm"] is None else f"{r['warm']:9.2f}"
        print(f"{name:10} {r['status']:8} {cold:9.2f} {warm_disp:>9} {str(r['rows']):>9}  {r['gpu']}")
        f.write(f"{name},{r['status']},{cold:.3f},{r['rows']}\n"); f.flush()
        fd.write(f"{name},{r['status']},{cold:.3f},{warm_s},{r['rows']},{r['gpu']}\n"); fd.flush()
        if r["status"] == "OK":
            ok += 1
    f.close(); fd.close()
    print(f"\n{ok}/{len([n for n in SUBSET if n in queries])} queries OK | csv -> {OUT_CSV}")
    sys.exit(0 if ok == len([n for n in SUBSET if n in queries]) else 1)


if __name__ == "__main__":
    main()
