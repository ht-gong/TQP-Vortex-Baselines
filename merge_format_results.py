#!/usr/bin/env python
"""Upsert one engine-run's per-query CSV into results/format_ablation.csv.

  merge_format_results.py <engine> <sf> <variant> <round> <run_csv> [results_dir]

The parquet-format ablation (results/FORMAT_ABLATION.md) adds two dimensions
that results/all_results.csv does not have -- the dataset's parquet format
variant and the evaluation round -- so it keeps its own table instead of
overloading the (engine, scale_factor) contract. One row per
(engine, scale_factor, variant, round, query); an existing slice for the same
(engine, sf, variant, round) is replaced. Same per-row fields as
all_results.csv, plus the variant decomposed into layout/encoding/compression.

<variant> is "<layout>-<encoding>-<compression>", e.g. shuffle-plain-zstd:
  layout       shuffle (round-robin repartition) | keyorder (sorted by PK)
  encoding     plain | dict (RLE_DICTIONARY w/ PLAIN fallback) | delta
               (DELTA_BINARY_PACKED ints, DELTA_BYTE_ARRAY strings)
  compression  snappy | zstd | lz4raw
"""
import csv, os, sys

FIELDS = ["engine", "scale_factor", "variant", "layout", "encoding", "compression",
          "round", "query", "status", "seconds", "rows_or_error"]
ENGINE_ORDER = ["duckdb_cpu", "sirius", "polars_gpu", "rapids", "polars_cpu"]


def main():
    if len(sys.argv) < 6:
        sys.exit(__doc__)
    engine, sf, variant, rnd, run_csv = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5]
    resdir = sys.argv[6] if len(sys.argv) > 6 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "results")
    out = os.path.join(resdir, "format_ablation.csv")
    layout, encoding, compression = variant.split("-")

    rows = []
    if os.path.exists(out):
        for r in csv.DictReader(open(out)):
            if (r["engine"] == engine and int(r["scale_factor"]) == sf
                    and r["variant"] == variant and int(r["round"]) == rnd):
                continue
            rows.append(r)
    n = 0
    for r in csv.DictReader(open(run_csv)):
        rows.append({"engine": engine, "scale_factor": sf, "variant": variant,
                     "layout": layout, "encoding": encoding, "compression": compression,
                     "round": rnd, "query": r["query"], "status": r["status"],
                     "seconds": r["seconds"],
                     "rows_or_error": r.get("result_rows_or_error", "")})
        n += 1

    def key(r):
        e = ENGINE_ORDER.index(r["engine"]) if r["engine"] in ENGINE_ORDER else 99
        q = int("".join(c for c in r["query"] if c.isdigit()) or 0)
        return (e, int(r["scale_factor"]), r["variant"], int(r["round"]), q)
    rows.sort(key=key)

    os.makedirs(resdir, exist_ok=True)
    tmp = out + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in FIELDS})
    os.replace(tmp, out)
    print(f"merged {engine} sf{sf} {variant} round{rnd}: {n} rows -> {out} ({len(rows)} total)")


if __name__ == "__main__":
    main()
