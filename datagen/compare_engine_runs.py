#!/usr/bin/env python3
"""Compare per-engine TPC-H runs over two datasets (from verify_engines.sh).

  compare_engine_runs.py <out_dir> [--label-a upstream] [--label-b datagen]
                         [--report engine_report.md]

Reads <out_dir>/<engine>__<label>.csv (the runners' native CSVs: columns
query,status,seconds,[gpu_ops,]result_rows_or_error) and checks:

  1. per engine: dataset A and dataset B give the same status and the same
     result row count for every query (the engine cannot tell them apart);
  2. across engines: every query that is OK everywhere returns the same row
     count on every engine (cross-engine correctness, per AGENTS.md).

Timings are tabulated side by side for information only. Exit 1 on any mismatch.
"""
import argparse
import csv
import glob
import os
import sys


def load(path):
    out = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            out[r["query"]] = (r["status"], r.get("result_rows_or_error", ""), r["seconds"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir")
    ap.add_argument("--label-a", default="upstream"); ap.add_argument("--label-b", default="datagen")
    ap.add_argument("--report")
    a = ap.parse_args()

    runs = {}   # engine -> {label: {query: (status, rows, secs)}}
    for p in sorted(glob.glob(os.path.join(a.out_dir, "*__*.csv"))):
        base = os.path.basename(p)[:-4]
        if base.endswith("_detail"):
            continue
        eng, lab = base.split("__", 1)
        runs.setdefault(eng, {})[lab] = load(p)

    lines, fails = [], []
    def out(s=""):
        lines.append(s); print(s)

    out(f"# Engine verification: {a.label_a} vs {a.label_b}")
    queries = [f"query{i}" for i in range(1, 23)]
    for eng in sorted(runs):
        ra, rb = runs[eng].get(a.label_a), runs[eng].get(a.label_b)
        out(f"\n## {eng}")
        if ra is None or rb is None:
            out(f"  [FAIL] missing run for {a.label_a if ra is None else a.label_b}")
            fails.append(f"{eng}: missing run"); continue
        out(f"  {'query':8} {'status A/B':14} {'rows A':>10} {'rows B':>10} {'secs A':>9} {'secs B':>9}")
        for q in queries:
            if q not in ra and q not in rb:
                continue
            sa, xa, ta = ra.get(q, ("MISSING", "", ""))
            sb, xb, tb = rb.get(q, ("MISSING", "", ""))
            same = (sa == sb) and (sa != "OK" or xa == xb)
            mark = "" if same else "   <-- MISMATCH"
            out(f"  {q:8} {sa + '/' + sb:14} {xa[:10]:>10} {xb[:10]:>10} {ta:>9} {tb:>9}{mark}")
            if not same:
                fails.append(f"{eng} {q}: {a.label_a}={sa}/{xa} {a.label_b}={sb}/{xb}")
            if sa != "OK":
                out(f"           error A: {xa[:120]}")
            if sb != "OK" and xb != xa:
                out(f"           error B: {xb[:120]}")

    out("\n## Cross-engine row counts (per dataset, OK queries only)")
    for lab in (a.label_a, a.label_b):
        out(f"\n### {lab}")
        engs = [e for e in sorted(runs) if lab in runs[e]]
        out("  " + f"{'query':8}" + "".join(f"{e:>12}" for e in engs))
        for q in queries:
            vals = {e: runs[e][lab].get(q) for e in engs}
            cells, oks = [], set()
            for e in engs:
                v = vals[e]
                if v is None:
                    cells.append(f"{'-':>12}")
                elif v[0] == "OK":
                    cells.append(f"{v[1]:>12}"); oks.add(v[1])
                else:
                    cells.append(f"{v[0]:>12}")
            mark = "" if len(oks) <= 1 else "   <-- ROW COUNT DISAGREEMENT"
            out(f"  {q:8}" + "".join(cells) + mark)
            if len(oks) > 1:
                fails.append(f"{lab} {q}: engines disagree on row count {sorted(oks)}")

    out("\n## Verdict")
    if fails:
        out(f"NOT EQUIVALENT -- {len(fails)} mismatch(es):")
        for f in fails:
            out(f"  - {f}")
    else:
        out(f"EQUIVALENT: every engine returns identical status and row counts on "
            f"{a.label_a} and {a.label_b}, and all engines agree with each other.")
    if a.report:
        with open(a.report, "w") as fh:
            fh.write("\n".join(lines) + "\n")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
