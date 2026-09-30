#!/usr/bin/env python3
"""Pivot results/format_ablation.csv into the tables in results/FORMAT_ABLATION.md.

  format_ablation_report.py [format_ablation.csv] [--sf 100] [--sizes <fmt_root>]

Per (engine, variant): median over rounds of each query's cold seconds, then
summed over q1..q22 (queries that failed in any round are counted separately),
plus min/max of the per-round totals to show run-to-run spread. Also prints the
encoding x compression grid per engine, keyorder vs shuffle, and, with
--sizes, on-disk bytes per variant. If format_ablation_colsizes.csv sits next to the
input CSV, appends the per-column size analysis (findings + tables A-D produced by
format_ablation_columns.py, which needs a python with the duckdb module).
"""
import argparse, csv, os, re, statistics, subprocess, sys
from collections import defaultdict

ENGINES = ["duckdb_cpu", "sirius", "polars_gpu", "rapids", "polars_cpu"]
ENC = ["plain", "dict", "delta"]
CODEC = ["snappy", "zstd", "lz4raw"]


def load(path, sf):
    rows = [r for r in csv.DictReader(open(path)) if int(r["scale_factor"]) == sf]
    return rows


def per_cell(rows):
    """(engine, variant) -> {query: [seconds per round]}, plus failure set."""
    cell = defaultdict(lambda: defaultdict(list))
    fail = defaultdict(set)
    rounds = defaultdict(set)
    for r in rows:
        k = (r["engine"], r["variant"])
        rounds[k].add(int(r["round"]))
        if r["status"] == "OK":
            cell[k][r["query"]].append(float(r["seconds"]))
        else:
            fail[k].add(r["query"])
    return cell, fail, rounds


def totals(cell, fail, rounds):
    """(engine, variant) -> dict(median_total, min_total, max_total, nq, nfail, nrounds)."""
    out = {}
    for k, qs in cell.items():
        ok_q = [q for q in qs if q not in fail[k]]
        med = sum(statistics.median(qs[q]) for q in ok_q)
        by_round = defaultdict(float)
        for q in ok_q:
            for i, s in enumerate(qs[q]):
                by_round[i] += s
        out[k] = dict(median_total=med, min_total=min(by_round.values()) if by_round else 0,
                      max_total=max(by_round.values()) if by_round else 0,
                      nq=len(ok_q), nfail=len(fail[k]), nrounds=len(rounds[k]))
    return out


def fmt(x):
    return f"{x:8.1f}" if isinstance(x, float) else f"{x:>8}"


COLUMN_FINDINGS = """\
## Per-column size analysis (SF100): where the bytes move, and whether bytes predict time

Data: `format_ablation_colsizes.csv` (compressed/encoded bytes and encodings per variant x
table x column, summed from every parquet footer by `format_ablation_colsizes.py`);
tables A-D below are produced by `format_ablation_columns.py`.

- **Encoding movers.** Everything that moves is in lineitem. The four PLAIN-fallback INT64
  columns (l_suppkey, l_orderkey, l_partkey, l_extendedprice; 2.8-3.1 GB each under
  plain/dict) drop to 0.53-0.60x with DELTA_BINARY_PACKED (~1.25 GB saved each). The
  low-cardinality columns move the other way: dictionary takes l_shipinstruct / l_shipmode /
  l_linestatus / l_returnflag to 0.12-0.19x of plain, while DELTA_BYTE_ARRAY on the same
  columns is 3-10x *larger* than dictionary (l_shipinstruct 146 MB -> 1446 MB). Dates are a
  tie (dict 0.48x of plain, delta 0.50x). Comments (l_/o_/ps_comment, ~13.4 GB, 38% of the
  dict/snappy dataset) barely move with encoding (delta 0.92-0.97x) and are mostly not read
  by the queries.
- **Best encoding per column** (compressed, snappy): every high-cardinality number is best
  under delta (19/19); every dictionary-encodable column is best under dict (21/24, the 3
  exceptions are ties within 5%); high-cardinality strings are best under delta by 5-10%
  (prefix sharing) except phones (plain). PLAIN never wins outright. A per-column mix would
  be 27.6 GiB vs the best uniform variant, 31.8 GiB (delta) / 35.4 GiB (dict).
- **Key order** only compresses the sort keys, but dramatically: l_orderkey delta
  1474 -> 182 MB, o_orderkey 367 -> 5 MB, ps_partkey 149 -> 1 MB; dictionary also starts
  fitting for l_orderkey (0.53x). Whole dataset only 0.92-0.95x because comments dominate.
- **Codecs are not encoding-neutral.** Ratio compressed/encoded: plain 0.41 (snappy) /
  0.25 (zstd), dict 0.52 / 0.36, delta 0.53 / 0.42. The codec substitutes for the encoding
  rather than stacking on it: on dictionary-able columns plain+zstd reaches 0.16 but still
  lands at 7.5 GiB vs 4.7 GiB for dict, which the codec cannot shrink further (1.00 snappy,
  0.96 zstd). Delta output is the least compressible (bit-packed deltas), so delta+zstd
  (25.6 GiB) ends up larger than dict+zstd (24.8 GiB) although delta+snappy is smaller than
  dict+snappy. lz4raw ~= snappy (+3-5%). With zstd all encodings land within
  24.8-27.6 GiB; with snappy they spread 31.8-45.4 GiB.
- **Size vs time.** Within a query, across the 12 variants, bytes of the touched columns
  predict median seconds for Sirius (mean r 0.78, 18/22 queries r>0.5) and polars_gpu
  (0.60, 17/22) but not DuckDB (0.30) or RAPIDS (0.05). Per factor: Sirius responds to
  both codec bytes (r 0.50) and encoding bytes (0.75), i.e. it is byte-bound (~0.34 s per
  GiB of dataset); polars_gpu responds only to encoding bytes (0.87), not codec bytes
  (-0.07), so zstd's smaller files do not help it (GPU zstd decompression plus the
  streaming-partition effect dominate); DuckDB's CPU decompression cost cancels the byte
  saving (codec r -0.12; lz4 is its fastest codec); RAPIDS is flat under every factor
  (Spark overhead). Whole-dataset size is a weak predictor everywhere because 38% of the
  bytes are comment columns the queries never read.
"""


def column_section(csv_path):
    here = os.path.dirname(os.path.abspath(__file__))
    col_csv = os.path.join(os.path.dirname(os.path.abspath(csv_path)), "format_ablation_colsizes.csv")
    if not os.path.exists(col_csv):
        return
    print("\n" + COLUMN_FINDINGS)
    r = subprocess.run([sys.executable, os.path.join(here, "format_ablation_columns.py")],
                       capture_output=True, text=True, cwd=here)
    if r.returncode != 0:
        print(f"(tables A-D not rendered: format_ablation_columns.py failed under {sys.executable}; "
              f"run with a python that has duckdb, e.g. datagen-venv/bin/python)\n")
        return
    # demote its '## X.' headings under this section
    print(re.sub(r"^## ", "### ", r.stdout, flags=re.M))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", default=os.path.join(os.path.dirname(__file__), "format_ablation.csv"))
    ap.add_argument("--sf", type=int, default=100)
    ap.add_argument("--sizes", default=None, help="fmt_sf<SF> root with <variant>/parquet dirs")
    a = ap.parse_args()
    rows = load(a.csv, a.sf)
    cell, fail, rounds = per_cell(rows)
    T = totals(cell, fail, rounds)
    engines = [e for e in ENGINES if any(k[0] == e for k in T)]
    variants = sorted({k[1] for k in T}, key=lambda v: (v.split("-")[0] != "shuffle",
                                                       CODEC.index(v.split("-")[2]), ENC.index(v.split("-")[1])))

    print(f"# SF{a.sf} parquet-format ablation: sum over q1-22 of per-query median cold seconds "
          f"(min..max of per-round totals); [n] = failed queries excluded\n")
    print("| variant | " + " | ".join(engines) + " |")
    print("|---|" + "---|" * len(engines))
    for v in variants:
        cells = []
        for e in engines:
            t = T.get((e, v))
            if not t:
                cells.append("—"); continue
            s = f"{t['median_total']:.1f} ({t['min_total']:.1f}..{t['max_total']:.1f})"
            if t["nfail"]:
                s += f" [{t['nfail']} FAIL]"
            if t["nrounds"] != max(len(x) for x in rounds.values()):
                s += f" (r={t['nrounds']})"
            cells.append(s)
        print(f"| {v} | " + " | ".join(cells) + " |")

    # encoding x codec grid per engine, shuffle layout, relative to dict-snappy
    for e in engines:
        base = T.get((e, "shuffle-dict-snappy"))
        if not base:
            continue
        print(f"\n## {e}: shuffle layout, encoding x compression (total s, x = vs dict/snappy)\n")
        print("| encoding | " + " | ".join(CODEC) + " |")
        print("|---|" + "---|" * len(CODEC))
        for enc in ENC:
            cells = []
            for c in CODEC:
                t = T.get((e, f"shuffle-{enc}-{c}"))
                cells.append(f"{t['median_total']:.1f} ({t['median_total']/base['median_total']:.2f}x)" if t else "—")
            print(f"| {enc} | " + " | ".join(cells) + " |")
        print("\n| encoding | keyorder-snappy | vs shuffle |")
        print("|---|---|---|")
        for enc in ENC:
            k, s = T.get((e, f"keyorder-{enc}-snappy")), T.get((e, f"shuffle-{enc}-snappy"))
            if k and s:
                print(f"| {enc} | {k['median_total']:.1f} | {k['median_total']/s['median_total']:.2f}x |")

    # per-query medians, one table per engine (variants as columns)
    for e in engines:
        print(f"\n## {e}: per-query median cold seconds\n")
        vs = [v for v in variants if (e, v) in T]
        print("| query | " + " | ".join(vs) + " |")
        print("|---|" + "---|" * len(vs))
        for qn in range(1, 23):
            q = f"query{qn}"
            cells = []
            for v in vs:
                xs = cell[(e, v)].get(q, [])
                if q in fail[(e, v)]:
                    cells.append(f"FAIL({len(xs)}/{len(rounds[(e, v)])} ok)")
                else:
                    cells.append(f"{statistics.median(xs):.2f}" if xs else "—")
            print(f"| {q} | " + " | ".join(cells) + " |")

    if a.sizes:
        print("\n## on-disk size per variant (GiB)\n\n| variant | total | lineitem |\n|---|---|---|")
        for v in variants:
            d = os.path.join(a.sizes, v, "parquet")
            if not os.path.isdir(d):
                continue
            tot = subprocess.run(["du", "-sb", d], capture_output=True, text=True).stdout.split()[0]
            li = subprocess.run(["du", "-sb", os.path.join(d, "lineitem")], capture_output=True, text=True).stdout.split()[0]
            print(f"| {v} | {int(tot)/2**30:.1f} | {int(li)/2**30:.1f} |")
    column_section(a.csv)


if __name__ == "__main__":
    main()
