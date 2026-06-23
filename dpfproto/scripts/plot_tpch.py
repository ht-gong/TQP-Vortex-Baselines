#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("log_dir")
    p.add_argument("--out", default="tpch_runtime.png")
    args = p.parse_args()

    rows = []
    for f in Path(args.log_dir).glob("sf*/*/q*.txt"):
        m = re.search(r"time:\s+avg=([0-9.]+)\s+min=([0-9.]+)\s+max=([0-9.]+)\s+stddev=([0-9.]+)", f.read_text(errors="replace"))
        if not m:
            continue
        rows.append({
            "sf": f.parts[-3].removeprefix("sf"),
            "mode": f.parts[-2],
            "query": f.stem,
            "avg_ms": float(m.group(1)),
        })

    if not rows:
        raise SystemExit(f"No query timings found under {args.log_dir}")

    df = pd.DataFrame(rows)
    df["query_n"] = df["query"].str.extract(r"q(\d+)").astype(int)
    df = df.sort_values(["query_n", "mode"])
    df.to_csv(Path(args.out).with_suffix(".csv"), index=False)

    ax = df.pivot(index="query", columns="mode", values="avg_ms").plot(kind="bar", figsize=(9, 4.5))
    ax.set_xlabel("TPC-H query")
    ax.set_ylabel("Average runtime (ms)")
    ax.set_title("DPFProto TPC-H")
    plt.tight_layout()
    plt.savefig(args.out, dpi=200)


if __name__ == "__main__":
    main()
