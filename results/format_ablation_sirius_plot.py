# Sirius: does dictionary or delta encoding cut runtime vs PLAIN? Per TPC-H SF100 query, bytes of the
# columns it reads vs Sirius median cold time (3 rounds), dictionary and delta relative to PLAIN; codec
# (snappy) and layout (shuffle) fixed so only the encoding varies. From format_ablation_colsizes.csv,
# format_ablation.csv and queries/stream_qualification.sql (same column matching as
# format_ablation_columns.py, section D):
#   python3 format_ablation_sirius_plot.py [out.png]   (needs matplotlib)
import csv, math, os, re, statistics as st, sys
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "format_ablation_sirius_size_time.png")
REF, VARIANTS = "shuffle-plain-snappy", {"dict": "shuffle-dict-snappy", "delta": "shuffle-delta-snappy"}
NAME = {"dict": "dictionary", "delta": "delta"}
FASTER = 0.97                       # counted as faster than PLAIN below this time ratio

SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
COLOR = {"plain": "#2a78d6", "dict": "#eb6834", "delta": "#1baf7a"}

sizes = [r for r in csv.DictReader(open(os.path.join(HERE, "format_ablation_colsizes.csv")))
         if r["column"] != "ignore"]
cols = {r["column"] for r in sizes}
parts = re.split(r"-- Template file: (\d+)", open(os.path.join(HERE, "queries/stream_qualification.sql")).read())
qcols = {int(parts[i]): {c for c in cols if re.search(r"\b" + c + r"\b", parts[i + 1])}
         for i in range(1, len(parts), 2)}
qbytes = defaultdict(int)          # (variant, query) -> on-disk bytes of the columns it reads
for r in sizes:
    for qn, cc in qcols.items():
        if r["column"] in cc:
            qbytes[(r["variant"], qn)] += int(r["compressed"])
runs = defaultdict(list)
for r in csv.DictReader(open(os.path.join(HERE, "format_ablation.csv"))):
    if r["scale_factor"] == "100" and r["engine"] == "sirius":
        runs[(r["variant"], int(r["query"][5:]))].append(float(r["seconds"]) if r["status"] == "OK" else None)
med = {k: st.median(v) for k, v in runs.items() if len(v) == 3 and None not in v}

pts = {e: [(qn, qbytes[(v, qn)] / qbytes[(REF, qn)], med[(v, qn)] / med[(REF, qn)])
           for qn in sorted(qcols)] for e, v in VARIANTS.items()}
xs = [x for e in pts for _, x, _ in pts[e]]
ys = [y for e in pts for _, _, y in pts[e]]
mx, my = st.mean(xs), st.mean(ys)
sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
r = sxy / math.sqrt(sum((a - mx) ** 2 for a in xs) * sum((b - my) ** 2 for b in ys))
slope = sxy / sum((a - mx) ** 2 for a in xs)
total = {v: sum(med[(v, qn)] for qn in qcols) for v in list(VARIANTS.values()) + [REF]}

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
                     "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
                     "axes.edgecolor": AXIS, "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
fig, ax = plt.subplots(figsize=(9, 6.2))
fig.subplots_adjust(left=0.09, right=0.97, top=0.80, bottom=0.14)
ax.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
ax.tick_params(length=0)
ax.set_xlim(0.45, 1.06)
ax.set_ylim(0.55, 1.2)
ax.axhline(1, color=AXIS, linewidth=1)
ax.axvline(1, color=AXIS, linewidth=1)
ax.plot([1], [1], "o", ms=9, color=COLOR["plain"], mec=SURFACE, mew=2, zorder=4)
ax.annotate("PLAIN = 1.0 for every query", (1, 1), xytext=(0.87, 1.14), textcoords="data",
            fontsize=8.5, color=INK2, arrowprops=dict(arrowstyle="-", color=MUTED, linewidth=0.8,
                                                      shrinkA=2, shrinkB=6))
ax.text(0.715, 1.005, "↑ slower than PLAIN", fontsize=8, color=MUTED, va="bottom")
ax.text(0.715, 0.995, "↓ faster than PLAIN", fontsize=8, color=MUTED, va="top")
fx = [0.45, 1.06]
ax.plot(fx, [my + slope * (x - mx) for x in fx], color=INK2, linewidth=1.2, zorder=2,
        label=f"linear fit over both encodings, r = {r:.2f}")
for e, v in VARIANTS.items():
    n_fast = sum(y < FASTER for _, _, y in pts[e])
    ax.scatter([x for _, x, _ in pts[e]], [y for _, _, y in pts[e]], s=64, color=COLOR[e],
               edgecolors=SURFACE, linewidths=1.5, zorder=3,
               label=f"{NAME[e]}: faster on {n_fast}/22 queries; all 22 take {total[v]:.1f} s vs "
                     f"{total[REF]:.1f} s PLAIN ({total[v] / total[REF]:.2f}×)")
for e, qn, dx, dy in [("delta", 9, 0.012, 0.0), ("delta", 5, 0.012, 0.0), ("delta", 15, -0.012, 0.0),
                      ("dict", 1, -0.012, 0.0), ("delta", 8, -0.012, 0.0), ("dict", 13, -0.012, 0.0)]:
    _, x, y = next(p for p in pts[e] if p[0] == qn)
    ax.text(x + dx, y + dy, f"Q{qn}", fontsize=8, color=INK2, va="center",
            ha="right" if dx < 0 else "left")
ax.set_xlabel("bytes the query reads, relative to PLAIN")
ax.set_ylabel("Sirius runtime, relative to PLAIN")
ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.17), frameon=False, fontsize=8.5,
          handletextpad=0.3, borderaxespad=0, ncol=1)
fig.suptitle("Sirius: dictionary and delta both beat PLAIN; delta more, because it reads less",
             x=0.09, y=0.975, ha="left", fontsize=12, fontweight="bold")
fig.text(0.09, 0.015, f"TPC-H SF100, one dot per query per encoding, median of 3 cold runs; 'faster' = "
         f"under {FASTER:.2f}× PLAIN. Compression (snappy)\nand row order are the same in every "
         "dataset; only the encoding changes.", fontsize=7.5, color=MUTED)
fig.savefig(OUT, dpi=200)
print(OUT, f"r={r:.3f} slope={slope:.3f}", {v: round(t, 1) for v, t in total.items()})
