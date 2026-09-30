#!/usr/bin/env python3
"""Parquet-format profiling pass: per engine, the fastest format for each column.

For one scale factor and a candidate set of encodings x codecs, each candidate
format is one uniform variant dataset of the same rows, named
shuffle-<encoding>-<codec> under $DATA_DIR/fmt_sf<SF>/. No TPC-H queries run.
Instead every engine scans and decodes every column of every variant with one
column probe, `SELECT min(c), max(c) FROM <table>` (ablation/probe_*.py), and
the fastest eligible format per (engine, column) wins.

  format_profile.py run    --sf SF [--encodings "plain dict delta"]
                           [--codecs "snappy zstd lz4raw"] [--rounds 3]
                           [--engines "rapids polars_gpu duckdb_cpu sirius"]
                           [--tol 0.05] [--out DIR]
  format_profile.py select --sf SF [--tol 0.05] [--out DIR]

Stages of `run`:
  1. variants   generate the missing ones from one shared dbgen output
                ($DATA_DIR/fmt_sf<SF>/_rawstore) with datagen/gen_tpch.sh, which
                validates and publishes each only when complete; existing ones
                are validated and reused.
  2. sizes      compressed bytes per column per variant from the parquet footers
                -> <out>/format_ablation_colsizes.csv
  3. probes     per round, per variant (staged disk -> ramdisk once), per engine:
                one worker process probes every column in a shuffled order,
                engine caches off. A probe that fails, times out
                ($PROBE_TIMEOUT s, default 30 + SF/2) or leaves the engine's native
                path (FALLBACK) makes that format ineligible for that column
                on that engine. -> <out>/format_ablation_profile.csv
  4. select     (also `select` alone) -> <out>/format_map.json and
                <out>/format_map_sf<SF>.md. Eligible: OK in every round. Score:
                median seconds over rounds. Formats within TOL of the fastest
                count as tied and the one with the fewest bytes wins.

Env: DATA_DIR (datasets), SCRATCH (engine scratch goes to $SCRATCH/probe),
SHM (ramdisk root, default /dev/shm), GPU or CUDA_VISIBLE_DEVICES (the one GPU
every GPU engine uses), PROBE_TIMEOUT, GEN_PARALLEL / GEN_BATCH (dbgen chunking
for new variants, as run.sh), and the image's tool variables.
"""
import argparse
import csv
import glob
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TABLES = ["lineitem", "orders", "partsupp", "part", "customer", "supplier", "nation", "region"]
ENCODINGS = {  # gen_tpch.py transcode flags per encoding
    "plain": "--dictionary false --writer-version v1",
    "dict": "--dictionary true --writer-version v1",
    "delta": "--dictionary false --writer-version v2",
}
CODECS = ["snappy", "zstd", "lz4raw"]
ENGINES = ["rapids", "polars_gpu", "duckdb_cpu", "sirius"]
DEFAULT_FORMAT = "dict-snappy"   # what the generator writes for the benchmark
GPU_ENGINES = {"rapids", "polars_gpu", "sirius"}

SIZE_FIELDS = ["scale_factor", "variant", "table", "column", "ptype", "compressed",
               "uncompressed", "num_values", "encodings"]
PROFILE_FIELDS = ["engine", "scale_factor", "variant", "encoding", "codec", "round",
                  "table", "column", "status", "seconds", "detail"]


def log(msg):
    print(f"[{time.strftime('%F %T')}] {msg}", flush=True)


def variant(enc, codec):
    return f"shuffle-{enc}-{codec}"


def fmt_of(v):
    return v.split("-", 1)[1]   # shuffle-dict-snappy -> dict-snappy


def data_dir():
    d = os.environ.get("DATA_DIR")
    if not d:
        sys.exit("DATA_DIR is not set")
    return d


def upsert(path, fields, rows, key, sort_key):
    """Replace every existing row whose key(row) is among the new rows' keys."""
    keys = {key(r) for r in rows}
    old = []
    if os.path.exists(path):
        with open(path) as f:
            rd = csv.DictReader(f)
            if rd.fieldnames != fields:
                sys.exit(f"{path}: columns {rd.fieldnames} != {fields}; not merging into it")
            old = [r for r in rd if key(r) not in keys]
    allrows = sorted(old + [{k: str(r[k]) for k in fields} for r in rows], key=sort_key)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(allrows)
    os.replace(tmp, path)


# ------------------------------------------------------------------ 1. variants
def ensure_variants(sf, variants):
    root = os.path.join(data_dir(), f"fmt_sf{sf}")
    par = int(os.environ.get("GEN_PARALLEL", max(20, 2 * sf)))
    batch = int(os.environ.get("GEN_BATCH", 25))
    for v in variants:
        pqdir = os.path.join(root, v, "parquet")
        if os.path.isdir(pqdir):
            log(f"variant {v}: exists")
        else:
            enc, codec = v.split("-")[1:]
            log(f"variant {v}: generating (PARALLEL={par} BATCH={batch}, raw store {root}/_rawstore)")
            env = dict(os.environ, RAW_STORE=os.path.join(root, "_rawstore"),
                       TRANSCODE_OPTS=f"--compression {codec} {ENCODINGS[enc]}")
            subprocess.run([os.path.join(ROOT, "datagen", "gen_tpch.sh"), str(sf), str(par),
                            str(batch), os.path.join(root, v)], env=env, check=True,
                           stdout=subprocess.DEVNULL)
        r = subprocess.run([sys.executable, os.path.join(ROOT, "results", "validate_dataset.py"),
                            pqdir, str(sf)], capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"variant {v} failed validation:\n{r.stdout}{r.stderr}")
    return {v: os.path.join(root, v, "parquet") for v in variants}


# ------------------------------------------------------------------ 2. sizes
def _table_sizes(job):
    v, pqdir, t = job
    agg = {}
    for path in glob.glob(f"{pqdir}/{t}/*.parquet"):
        md = pq.ParquetFile(path).metadata
        for rg in range(md.num_row_groups):
            r = md.row_group(rg)
            for c in range(r.num_columns):
                cc = r.column(c)
                d = agg.setdefault(cc.path_in_schema, [0, 0, set(), cc.physical_type, 0])
                d[0] += cc.total_compressed_size
                d[1] += cc.total_uncompressed_size
                d[2].update(cc.encodings)
                d[4] += cc.num_values
    return [dict(variant=v, table=t, column=k, ptype=typ, compressed=cb, uncompressed=ub,
                 num_values=nv, encodings="|".join(sorted(e)))
            for k, (cb, ub, e, typ, nv) in agg.items()]


def column_sizes(sf, paths, out):
    jobs = [(v, p, t) for v, p in paths.items() for t in TABLES]
    rows = []
    with ProcessPoolExecutor(min(32, len(jobs))) as ex:
        for part in ex.map(_table_sizes, jobs):
            rows += [dict(r, scale_factor=sf) for r in part]
    upsert(os.path.join(out, "format_ablation_colsizes.csv"), SIZE_FIELDS, rows,
           key=lambda r: (int(r["scale_factor"]), r["variant"]),
           sort_key=lambda r: (int(r["scale_factor"]), r["variant"], TABLES.index(r["table"]), r["column"]))
    log(f"column sizes: {len(rows)} rows -> {out}/format_ablation_colsizes.csv")


# ------------------------------------------------------------------ 3. probes
def columns(pqdir):
    cols = []
    for t in TABLES:
        schema = pq.read_schema(sorted(glob.glob(f"{pqdir}/{t}/*.parquet"))[0])
        cols += [(t, c) for c in schema.names if c != "ignore"]
    return cols


def stage(src, shm, sf):
    ram = os.path.join(shm, f"tpch_fmt_sf{sf}")
    shutil.rmtree(ram, ignore_errors=True)
    os.makedirs(ram)
    dst = os.path.join(ram, "parquet")
    subprocess.run(["cp", "-r", src, dst], check=True)
    # Sirius' GPU reader rejects 0-row part-files; drop them for every engine (as run.sh).
    for f in glob.glob(f"{dst}/*/*.parquet"):
        if pq.ParquetFile(f).metadata.num_rows == 0:
            os.remove(f)
    return ram, dst


def run_worker(engine, pqdir, cols, run_dir, timeout, env):
    """Probe cols with one worker; restart it after a timeout or crash.
    Returns {(table, column): (status, seconds, detail)}."""
    results, remaining, attempt, no_prime = {}, list(cols), 0, False
    while remaining:
        attempt += 1
        cols_file = os.path.join(run_dir, f"columns.{attempt}.txt")
        out_csv = os.path.join(run_dir, f"probes.{attempt}.csv")
        with open(cols_file, "w") as f:
            f.writelines(f"{t} {c}\n" for t, c in remaining)
        # cur = the probe in flight: ("probe", table, column) or ("prime", ...)
        state = {"cur": None, "since": time.time()}
        logf = open(os.path.join(run_dir, f"worker.{attempt}.log"), "w")
        wenv = dict(env, PROBE_NO_PRIME="1" if no_prime else "0")
        proc = subprocess.Popen([os.path.join(HERE, "probe.sh"), engine, pqdir, cols_file, out_csv],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                env=wenv, start_new_session=True)

        def read():
            for line in proc.stdout:
                logf.write(line)
                logf.flush()
                word = line.split(" ", 1)[0]
                if word in ("PROBE-START", "PRIME-START"):
                    t, c = line.split()[1].split(".", 1)
                    state["cur"] = ("probe" if word == "PROBE-START" else "prime", t, c)
                    state["since"] = time.time()
                elif word in ("PROBE-END", "PRIME-END", "PROBE-ABORT"):
                    state["cur"] = None
        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        timed_out = None
        try:
            while proc.poll() is None:
                time.sleep(0.5)
                # a probe in flight may take `timeout`; worker startup up to 15 min
                limit = timeout if state["cur"] else max(timeout, 900)
                if time.time() - state["since"] > limit:
                    timed_out = state["cur"] or ("startup",)
                    os.killpg(proc.pid, 9)
                    proc.wait()
        finally:
            if proc.poll() is None:   # the driver itself is being stopped
                os.killpg(proc.pid, 9)
        reader.join(10)
        logf.close()

        progress = 0
        if os.path.exists(out_csv):
            with open(out_csv) as f:
                for t, c, status, secs, detail in csv.reader(f):
                    results[(t, c)] = (status, float(secs) if secs else None, detail)
                    progress += 1
        if timed_out and timed_out[0] == "probe":
            results[timed_out[1:]] = ("TIMEOUT", float(timeout), f"exceeded {timeout}s")
            progress += 1
        elif timed_out and timed_out[0] == "prime" and not no_prime:
            log(f"      priming probe {timed_out[1]}.{timed_out[2]} timed out; restarting without priming")
            no_prime = True
            progress += 1       # not a result, but a change worth another attempt
        elif state["cur"] and state["cur"][0] == "probe":   # died inside this probe
            results[state["cur"][1:]] = ("FAIL", None, f"worker exited rc={proc.returncode} during probe")
            progress += 1
        remaining = [tc for tc in remaining if tc not in results]
        if remaining and progress == 0:
            why = (f"worker {timed_out[0]} timed out" if timed_out
                   else f"worker exited rc={proc.returncode} before probing")
            for tc in remaining:
                results[tc] = ("FAIL", None, why)
            remaining = []
        elif remaining:
            log(f"      worker stopped ({'timeout' if timed_out else f'rc={proc.returncode}'}); "
                f"restarting on {len(remaining)} columns")
    return results


def probes(sf, paths, engines, rounds, out, seed):
    shm = os.environ.get("SHM", "/dev/shm")
    # spark-rapids hangs outright on some delta+zstd reads; a probe normally
    # takes seconds, so the cap stays short and grows with the data.
    timeout = int(os.environ.get("PROBE_TIMEOUT", 30 + sf // 2))
    if not os.environ.get("SCRATCH"):
        sys.exit("SCRATCH is not set")
    scratch = os.path.join(os.environ["SCRATCH"], "probe")
    env = dict(os.environ, PROBE_SCRATCH=scratch)
    gpu = os.environ.get("GPU") or os.environ.get("CUDA_VISIBLE_DEVICES")
    if GPU_ENGINES & set(engines) and not gpu:
        sys.exit("set GPU (or CUDA_VISIBLE_DEVICES) to the one free GPU the GPU engines may use")
    if gpu:
        env["CUDA_VISIBLE_DEVICES"] = gpu
    profile = os.path.join(out, "format_ablation_profile.csv")
    for rnd in range(1, rounds + 1):
        for v, src in paths.items():
            enc, codec = v.split("-")[1:]
            t0 = time.time()
            ram, pqdir = stage(src, shm, sf)
            cols = columns(pqdir)
            log(f"round {rnd} / {v}: staged in {time.time() - t0:.0f}s, {len(cols)} columns")
            try:
                for e in engines:
                    run_dir = os.path.join(out, "format_profile_logs", f"sf{sf}", f"r{rnd}", v, e)
                    shutil.rmtree(run_dir, ignore_errors=True)
                    os.makedirs(run_dir)
                    order = list(cols)
                    random.Random(f"{seed}:{sf}:{v}:{rnd}:{e}").shuffle(order)
                    env["PROBE_LOG_DIR"] = run_dir
                    t0 = time.time()
                    res = run_worker(e, pqdir, order, run_dir, timeout, env)
                    rows = [dict(engine=e, scale_factor=sf, variant=v, encoding=enc, codec=codec,
                                 round=rnd, table=t, column=c, status=st,
                                 seconds="" if s is None else f"{s:.4f}", detail=d)
                            for (t, c), (st, s, d) in res.items()]
                    upsert(profile, PROFILE_FIELDS, rows,
                           key=lambda r: (r["engine"], int(r["scale_factor"]), r["variant"], int(r["round"])),
                           sort_key=lambda r: (ENGINES.index(r["engine"]) if r["engine"] in ENGINES else 99,
                                               int(r["scale_factor"]), r["variant"], int(r["round"]),
                                               TABLES.index(r["table"]), r["column"]))
                    bad = {}
                    for st, _, _ in res.values():
                        if st != "OK":
                            bad[st] = bad.get(st, 0) + 1
                    log(f"    {e}: {len(res) - sum(bad.values())}/{len(res)} OK"
                        f"{' ' + str(bad) if bad else ''}, {time.time() - t0:.0f}s")
            finally:
                shutil.rmtree(ram, ignore_errors=True)


# ------------------------------------------------------------------ 4. select
def select(sf, tol, out):
    with open(os.path.join(out, "format_ablation_profile.csv")) as f:
        prof = [r for r in csv.DictReader(f) if int(r["scale_factor"]) == sf]
    with open(os.path.join(out, "format_ablation_colsizes.csv")) as f:
        size = {(r["variant"], r["table"], r["column"]): int(r["compressed"])
                for r in csv.DictReader(f) if int(r["scale_factor"]) == sf}
    if not prof:
        sys.exit(f"no SF{sf} rows in {out}/format_ablation_profile.csv")
    # (engine, table.column, format) -> list of (status, seconds) over rounds
    runs = {}
    for r in prof:
        k = (r["engine"], f"{r['table']}.{r['column']}", fmt_of(r["variant"]))
        runs.setdefault(k, []).append((r["status"], float(r["seconds"]) if r["seconds"] else None))
    engines = [e for e in ENGINES if any(k[0] == e for k in runs)] + \
              sorted({k[0] for k in runs} - set(ENGINES))
    formats = sorted({k[2] for k in runs})
    cols = sorted({k[1] for k in runs}, key=lambda c: (TABLES.index(c.split(".")[0]), c))
    rounds = sorted({int(r["round"]) for r in prof})

    def nbytes(col, fmt):
        t, c = col.split(".")
        return size.get((f"shuffle-{fmt}", t, c))

    score = {}   # (engine, col, fmt) -> median seconds, eligible only
    for (e, col, fmt), rs in runs.items():
        if rs and all(st == "OK" for st, _ in rs):
            score[(e, col, fmt)] = statistics.median(s for _, s in rs)

    fmap, report = {}, []
    for e in engines:
        fmap[e] = {}
        for col in cols:
            elig = {fmt: score[(e, col, fmt)] for fmt in formats if (e, col, fmt) in score}
            if not elig:
                fmap[e][col] = None
                continue
            best = min(elig.values())
            tied = [f for f, s in elig.items() if s <= best * (1 + tol)]
            pick = min(tied, key=lambda f: (nbytes(col, f) if nbytes(col, f) is not None else 1 << 62,
                                             elig[f], f))
            enc, codec = pick.split("-")
            fmap[e][col] = dict(format=pick, encoding=enc, codec=codec, seconds=round(elig[pick], 4),
                                bytes=nbytes(col, pick), eligible=len(elig), tied=len(tied))

    doc = {}
    path = os.path.join(out, "format_map.json")
    if os.path.exists(path):
        with open(path) as f:
            doc = json.load(f)
    doc[f"sf{sf}"] = dict(scale_factor=sf, tolerance=tol, rounds=rounds, formats=formats,
                          default_format=DEFAULT_FORMAT, engines=fmap)
    with open(path + ".tmp", "w") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    os.replace(path + ".tmp", path)

    # ---- report
    report += [f"# Format map, SF{sf}", "",
               f"Per-engine, per-column parquet format from `ablation/format_profile.py`: "
               f"one `SELECT min(c), max(c)` probe per (engine, column, format, round), "
               f"rounds {rounds}, candidate formats {', '.join(formats)}. A format is "
               f"eligible for a column when its probe was OK in every round; the score is "
               f"the median seconds; formats within {tol:.0%} of the fastest are tied and "
               f"the fewest compressed bytes wins.", "",
               "**Limit:** each choice is a per-column optimum measured in isolation, not a "
               "tested dataset. The pinned writer (parquet-mr via Spark 3.5.8) sets "
               "dictionary encoding per column, but codec and page version (v1/v2, which "
               "selects delta) apply to the whole file, so an arbitrary map cannot be "
               "written as one dataset with it.", ""]
    report += ["## Summary", "",
               "Summed probe seconds over all columns: the map vs the default format "
               f"(`{DEFAULT_FORMAT}`) and vs the best single uniform format. The map's sum "
               "is optimistic: each column takes the minimum of several noisy medians, so "
               "part of its lead over a uniform format is selection noise.", "",
               "| engine | map s | default s | best uniform | uniform s | columns ≠ default | no eligible format |",
               "|---|---:|---:|---|---:|---:|---:|"]
    for e in engines:
        m = fmap[e]
        chosen = sum(v["seconds"] for v in m.values() if v)
        missing = sum(1 for v in m.values() if v is None)
        uni = {fmt: sum(score[(e, c, fmt)] for c in cols) for fmt in formats
               if all((e, c, fmt) in score for c in cols)}
        default = f"{uni[DEFAULT_FORMAT]:.2f}" if DEFAULT_FORMAT in uni else "n/a"
        bu = min(uni, key=uni.get) if uni else None
        diff = sum(1 for v in m.values() if v and v["format"] != DEFAULT_FORMAT)
        report.append(f"| {e} | {chosen:.2f} | {default} | {bu or 'none'} | "
                      f"{uni[bu]:.2f} | {diff} | {missing} |" if bu else
                      f"| {e} | {chosen:.2f} | {default} | none | n/a | {diff} | {missing} |")
    bad = [r for r in prof if r["status"] != "OK"]
    if bad:
        report += ["", "## Ineligible probes", "",
                   "| engine | format | column | round | status | detail |", "|---|---|---|---:|---|---|"]
        for r in bad:
            report.append(f"| {r['engine']} | {fmt_of(r['variant'])} | {r['table']}.{r['column']} | "
                          f"{r['round']} | {r['status']} | {r['detail'][:80]} |")
    for e in engines:
        report += ["", f"## {e}", "", "| column | format | median s | bytes | tied | default s |",
                   "|---|---|---:|---:|---:|---:|"]
        for col in cols:
            v = fmap[e][col]
            d = score.get((e, col, DEFAULT_FORMAT))
            ds = f"{d:.4f}" if d is not None else "n/a"
            if v is None:
                report.append(f"| {col} | **none eligible** | | | | {ds} |")
            else:
                report.append(f"| {col} | {v['format']} | {v['seconds']:.4f} | {v['bytes']} | "
                              f"{v['tied']}/{v['eligible']} | {ds} |")
    md = os.path.join(out, f"format_map_sf{sf}.md")
    with open(md, "w") as f:
        f.write("\n".join(report) + "\n")
    missing = {e: sum(1 for v in fmap[e].values() if v is None) for e in engines}
    log(f"format map -> {path} (sf{sf}), report -> {md}; columns without an eligible format: {missing}")


def main():
    # SIGTERM -> SystemExit, so a stopped pass still kills its worker and ramdisk copy
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "select"):
        s = sub.add_parser(name)
        s.add_argument("--sf", type=int, required=True)
        s.add_argument("--tol", type=float, default=0.05)
        s.add_argument("--out", default=os.path.join(ROOT, "results"))
        if name == "run":
            s.add_argument("--encodings", default="plain dict delta")
            s.add_argument("--codecs", default="snappy zstd lz4raw")
            s.add_argument("--rounds", type=int, default=3)
            s.add_argument("--engines", default=" ".join(ENGINES))
            s.add_argument("--seed", default="0", help="column-order shuffle seed")
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.cmd == "run":
        encs, codecs, engines = a.encodings.split(), a.codecs.split(), a.engines.split()
        bad = [x for x in encs if x not in ENCODINGS] + [x for x in codecs if x not in CODECS] + \
              [x for x in engines if x not in ENGINES]
        if bad:
            sys.exit(f"unknown encoding/codec/engine: {bad}")
        variants = [variant(e, c) for e in encs for c in codecs]
        log(f"format profile SF{a.sf}: variants {variants}, engines {engines}, rounds {a.rounds} -> {a.out}")
        paths = ensure_variants(a.sf, variants)
        column_sizes(a.sf, paths, a.out)
        probes(a.sf, paths, engines, a.rounds, a.out, a.seed)
    select(a.sf, a.tol, a.out)


if __name__ == "__main__":
    main()
