# Share of the TPC-H SF100 data whose column is smallest under PLAIN / dictionary / delta. The winner is
# picked on compressed bytes (snappy, shuffle layout; the basis of FORMAT_ABLATION.md's "Best encoding
# per column"); each column is weighted by its raw size (PLAIN, uncompressed). A column whose dictionary
# overflowed is PLAIN-encoded in the dict variant, so it counts as PLAIN, not dictionary.
#   python3 format_ablation_best_encoding_plot.py [colsizes.csv] [out.png]   (needs matplotlib)
import csv, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "format_ablation_colsizes.csv")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "format_ablation_best_encoding.png")
ENCS = ["plain", "dict", "delta"]
NAME = {"plain": "PLAIN", "dict": "dictionary", "delta": "delta"}
KINDS = {"plain": "phone numbers", "dict": "flags, categories, dates, small numbers",
         "delta": "keys, prices, names, addresses, comments"}

SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
COLOR = {"plain": "#2a78d6", "dict": "#eb6834", "delta": "#1baf7a"}

cols = {}
for r in csv.DictReader(open(SRC)):
    lay, enc, codec = r["variant"].split("-")
    if lay == "shuffle" and codec == "snappy" and r["column"] != "ignore":
        cols.setdefault((r["table"], r["column"]), {})[enc] = (int(r["compressed"]), r["encodings"].split("|"),
                                                               int(r["uncompressed"]))
best = {e: [] for e in ENCS}
for k, v in cols.items():
    size = {e: v[e][0] for e in ENCS}
    if "PLAIN_DICTIONARY" not in v["dict"][1]:     # dictionary fell back to PLAIN for the whole column
        del size["dict"]
    best[min(size, key=size.get)].append(v["plain"][2])
total = sum(sum(b) for b in best.values())
GIB = 2**30

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
                     "axes.edgecolor": AXIS, "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
fig, ax = plt.subplots(figsize=(9, 3.6))
fig.subplots_adjust(left=0.33, right=0.97, top=0.80, bottom=0.17)
ys = list(range(len(ENCS)))[::-1]
for y, e in zip(ys, ENCS):
    pct = 100 * sum(best[e]) / total
    ax.barh(y, pct, height=0.5, color=COLOR[e], linewidth=0)
    ax.text(pct + 1, y, f"{pct:.{0 if pct >= 1 else 1}f}%  ({sum(best[e]) / GIB:.1f} GiB, {len(best[e])} columns)",
            va="center", fontsize=9.5, color=INK)
    ax.text(-0.02, y + 0.09, NAME[e], transform=ax.get_yaxis_transform(), ha="right", va="bottom",
            fontsize=10.5, fontweight="bold", color=INK)
    ax.text(-0.02, y + 0.03, KINDS[e], transform=ax.get_yaxis_transform(), ha="right", va="top",
            fontsize=8.5, color=INK2)
ax.set_xlim(0, 100)
ax.set_ylim(-0.6, len(ENCS) - 0.4)
ax.set_yticks([])
ax.set_xticks(range(0, 101, 25))
ax.set_xticklabels([f"{x}%" for x in range(0, 101, 25)], color=MUTED)
ax.xaxis.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
for s in ("top", "right", "bottom"):
    ax.spines[s].set_visible(False)
ax.tick_params(length=0)
fig.suptitle("How much of the TPC-H data is smallest under each encoding?", x=0.03, y=0.965, ha="left",
             fontsize=12, fontweight="bold")
fig.text(0.03, 0.855, f"Share of the SF100 data ({total / GIB:.0f} GiB as raw PLAIN values), by the "
         "encoding that makes each column smallest",
         fontsize=9, color=INK2)
fig.text(0.03, 0.02, "parquet-mr 1.13.1. Winner = smallest compressed size (snappy, rows in shuffled order); "
         "each column weighted by its raw PLAIN size.\nA column whose dictionary grew too large is stored as "
         "PLAIN, so it counts as PLAIN.", fontsize=7.5, color=MUTED)
fig.savefig(OUT, dpi=200)
print(OUT, {e: f"{100 * sum(v) / total:.1f}%" for e, v in best.items()}, f"{total / GIB:.1f} GiB")
