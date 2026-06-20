#!/usr/bin/env python3
"""Merge the GPU Spark-RAPIDS and GPU Polars result CSVs into one TPC-H q1-22
comparison table (markdown + CSV).

  make_table.py <spark_csv> <polars_csv> <out_md> <out_csv>
"""
import csv
import sys


def load(path):
    """{qnum: (seconds, status, rows)} from a results CSV (either schema)."""
    out = {}
    try:
        with open(path) as f:
            r = csv.reader(f)
            next(r, None)
            for row in r:
                if not row:
                    continue
                q = row[0].replace("query", "").strip()
                status = row[1].strip()
                secs = row[2].strip()
                rows = row[-1].strip()
                out[q] = (secs, status, rows)
    except FileNotFoundError:
        pass
    return out


def fmt(v):
    try:
        return f"{float(v):.2f}"
    except (ValueError, TypeError):
        return "—"


def main():
    spark_csv, polars_csv, out_md, out_csv = sys.argv[1:5]
    spark = load(spark_csv)
    polars = load(polars_csv)
    qs = sorted(set(spark) | set(polars), key=lambda x: int(x))

    rows, p_tot, s_tot = [], 0.0, 0.0
    for q in qs:
        ps, pst, prows = polars.get(q, ("", "NA", ""))
        ss, sst, srows = spark.get(q, ("", "NA", ""))
        if pst == "OK":
            try: p_tot += float(ps)
            except ValueError: pass
        if sst == "OK":
            try: s_tot += float(ss)
            except ValueError: pass
        match = "yes" if (prows and srows and prows == srows) else (
            "" if (prows == "" or srows == "") else "DIFF")
        rows.append((q, fmt(ps), pst, fmt(ss), sst, prows or srows, match))

    # CSV
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "polars_gpu_s", "polars_status",
                    "spark_rapids_gpu_s", "spark_status", "result_rows", "rows_match"])
        w.writerows(rows)

    # Markdown
    lines = [
        "# TPC-H GPU benchmark — Polars vs Spark-RAPIDS (queries 1–22)",
        "",
        "Per-query seconds, query execution only (engine/JVM/GPU startup excluded).",
        "",
        "| query | Polars GPU (s) | Spark-RAPIDS GPU (s) | rows | match |",
        "|------:|---------------:|---------------------:|-----:|:-----:|",
    ]
    for q, ps, pst, ss, sst, rws, m in rows:
        pcell = ps if pst == "OK" else f"{ps} ({pst})"
        scell = ss if sst == "OK" else f"{ss} ({sst})"
        lines.append(f"| q{q} | {pcell} | {scell} | {rws} | {m} |")
    lines += [
        f"| **Σ (OK)** | **{p_tot:.1f}** | **{s_tot:.1f}** | | |",
        "",
        f"- Polars GPU total: {p_tot:.1f} s   ·   Spark-RAPIDS GPU total: {s_tot:.1f} s",
        "- `match` = identical result row counts between the two engines.",
    ]
    with open(out_md, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
