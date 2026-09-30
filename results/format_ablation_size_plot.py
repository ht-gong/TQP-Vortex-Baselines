# Different encodings win on different columns: encoded (pre-compression) size of representative
# TPC-H SF100 columns under PLAIN / dictionary / delta, relative to PLAIN, from
# format_ablation_colsizes.csv ('uncompressed' = page bytes after encoding, before the codec):
#   python3 format_ablation_size_plot.py [colsizes.csv] [out.png]   (needs matplotlib)
import csv, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "format_ablation_colsizes.csv")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "format_ablation_size.png")
ENCS = ["plain", "dict", "delta"]
COLS = [("lineitem", "l_orderkey", "unique keys"),
        ("lineitem", "l_extendedprice", "prices"),
        ("customer", "c_name", "strings with\na shared prefix"),
        ("lineitem", "l_comment", "free text"),
        ("lineitem", "l_shipdate", "dates\n(~2.5k values)"),
        ("lineitem", "l_quantity", "small numbers\n(50 values)"),
        ("lineitem", "l_shipmode", "categories\n(7 strings)")]

SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
COLOR = {"plain": "#2a78d6", "dict": "#eb6834", "delta": "#1baf7a"}
LABEL = {"plain": "PLAIN", "dict": "dictionary", "delta": "delta"}

enc_bytes = {}
for r in csv.DictReader(open(SRC)):
    lay, enc, codec = r["variant"].split("-")
    if lay == "shuffle" and codec == "snappy":   # encoded bytes are codec-independent (<0.5%)
        enc_bytes[(r["table"], r["column"], enc)] = int(r["uncompressed"])

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": MUTED,
                     "axes.edgecolor": AXIS, "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
fig, ax = plt.subplots(figsize=(11, 5.2))
fig.subplots_adjust(left=0.07, right=0.985, top=0.80, bottom=0.17)
w = 0.26
for c, (t, col, _) in enumerate(COLS):
    rel = [enc_bytes[(t, col, e)] / enc_bytes[(t, col, "plain")] for e in ENCS]
    for i, (e, v) in enumerate(zip(ENCS, rel)):
        x = c + (i - 1) * w
        ax.bar(x, v, w * 0.86, color=COLOR[e], linewidth=0)
        win = v == min(rel)
        ax.text(x, v + 0.015, f"{v:.2f}", ha="center", va="bottom", fontsize=8,
                color=INK if win else INK2, fontweight="bold" if win else "normal")
ax.set_xticks(range(len(COLS)))
ax.set_xticklabels([f"{col}\n{desc}" for _, col, desc in COLS], fontsize=8.5)
ax.set_ylim(0, 1.12)
ax.set_ylabel("encoded size relative to PLAIN")
ax.yaxis.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
for s in ("top", "right", "left"):
    ax.spines[s].set_visible(False)
ax.tick_params(axis="y", length=0)
ax.tick_params(axis="x", length=0, pad=6)
ax.axvline(3.5, color=AXIS, linewidth=0.8)
ax.text(1.5, 1.09, "many distinct values: delta wins", ha="center", fontsize=9.5, color=INK)
ax.text(5.0, 1.09, "few distinct values: dictionary wins", ha="center", fontsize=9.5, color=INK)

handles = [plt.Rectangle((0, 0), 1, 1, color=COLOR[e]) for e in ENCS]
fig.legend(handles, [LABEL[e] for e in ENCS], loc="upper left", ncol=3, frameon=False,
           bbox_to_anchor=(0.065, 0.9), fontsize=9, handlelength=1.1, columnspacing=1.6)
fig.suptitle("No single parquet encoding wins: the best one depends on the column's data",
             x=0.07, y=0.975, ha="left", fontsize=12, fontweight="bold")
fig.text(0.07, 0.015, "TPC-H SF100, parquet-mr 1.13.1, bytes after encoding and before compression "
         "(bold = smallest). Dictionary = 1.00 means the dictionary grew too large and parquet-mr "
         "fell back to PLAIN.", fontsize=7.5, color=MUTED)
fig.savefig(OUT, dpi=200)
print(OUT)
