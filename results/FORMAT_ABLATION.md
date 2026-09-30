# Parquet-format ablation (SF100)

Does the parquet *format* -- page encoding, compression codec, and row order --
change the TPC-H runtime of the engines this repo compares? Every dataset here
holds the **same rows** (one dbgen run, transcoded by the same Spark/parquet-mr
pipeline as the certified data); only the writer knobs differ.

Tooling: `datagen/gen_format_variants.sh` (datasets), `run_format_ablation.sh`
(runs), `merge_format_results.py` -> `results/format_ablation.csv` (data),
`results/format_ablation_report.py` (the tables below).

Follow-up analysis of *why* -- how much of a dictionary / delta stream a query
needs, what can be skipped (read less vs decode less), and what Sirius/Simpatico,
DuckDB, cuDF and the state of the art do about it: `results/FORMAT_DECODING.md`
(`results/format_ablation_pagestats.py`, `results/format_ablation_decode.py`).

## Variants

`<layout>-<encoding>-<compression>`, written by parquet-mr 1.13.1 via Spark 3.5.8
(`datagen/gen_tpch.py transcode` knobs `--layout/--dictionary/--writer-version/--compression`):

| knob | values | parquet-mr setting | what the columns get |
|---|---|---|---|
| encoding | `dict` (reference) | dictionary on, page v1 | dictionary (RLE/bit-packed indices) where the dictionary pays off, parquet-mr's PLAIN fallback otherwise (high-cardinality keys, prices, comments) |
| | `plain` | dictionary off, page v1 | PLAIN for every column |
| | `delta` | dictionary off, page v2 | DELTA_BINARY_PACKED for INT32/INT64 (keys, dates, decimals), DELTA_BYTE_ARRAY for strings (parquet-mr offers no PLAIN-for-strings + delta-for-ints mix) |
| compression | `snappy` (reference) · `zstd` · `lz4raw` (LZ4_RAW) | `compression` option | per page |
| layout | `shuffle` (reference) | `repartition(200)` | round-robin: rows out of key order, 200 files per table per batch |
| | `keyorder` | `repartitionByRange(200, pk).sortWithinPartitions(pk)` | each file a contiguous, sorted primary-key range = dbgen input order |

"RLE/bit-packed" as a data-page encoding is only defined for BOOLEAN columns
(and levels/dictionary indices), which TPC-H has none of, so it is not a
variant; `dict` is where RLE/bit-packing appears.

Matrix: 3 encodings x 3 codecs with the shuffle layout (9), plus 3 encodings
at snappy in key order (3) = 12 datasets. Same file counts and row groups per
table as the reference (200 x 8 batches).

## Protocol

- Engines: `rapids`, `polars_gpu`, `duckdb_cpu`, `sirius` (GPU 5 of the 8xH100 box).
- **Cold everywhere**: one fresh process per query for the GPU engines (their
  normal runners); DuckDB in `views` mode with `RUNS=1 WARMUPS=0` so every query
  scans parquet (its usual `tables` mode would load the data once and hide the
  format entirely); Sirius `SIRIUS_ITERS=1`.
- 3 rounds, round-major (round 1 of all 12 variants, then round 2, ...), each
  variant staged disk -> ramdisk before its 4 engines run. Reported number =
  per-query **median over the 3 rounds**, summed over q1-q22.

## Generation

Verification round (2026-09-23, 8xH100 box, 208 cores, NVMe for raw text and
Spark local dirs; `PARALLEL=200 BATCH=25`, 8 batches):

| pipeline | wall | of which dbgen | of which Spark transcode | output |
|---|---|---|---|---|
| `datagen/gen_tpch.sh` (ours) | **620 s** (10.3 min) | 102 s | 518 s | 36 GB, VALID |
| `rapids/nds_h_pipeline.sh` (upstream NDS-H reference) | **1954 s** (32.6 min) | ~8 x 200 s | ~8 x 60 s | 36 GB, VALID |

Both use the byte-identical dbgen binary and the same Spark; the gap is the
upstream driver's raw step (`nds_h_gen_data.py` waits on dbgen with its output
un-suppressed and moves files afterwards), not the data. `datagen/verify_equivalence.py`
on the two SF100 datasets (`datagen/verification/equivalence_sf100.md`):
**EQUIVALENT** on all 8 tables -- identical Arrow/parquet schema, writer, per-column
codec and encoding sets, 1600 files and row groups per table, rows-per-file
multiset, 600,037,902 lineitem rows, `EXCEPT ALL` empty both ways.

The other 11 variants are transcoded from the same raw text
(`gen_tpch.sh` env `RAW_STORE`), so every variant holds exactly these rows.
Per-variant transcode (same raw text, Spark local[*], 8 batches; the
ablation's other 11 variants ran while an SF1 smoke test shared the box, so
these are +/- 10%) and resulting size, 1600 files per table each:

| variant | transcode | total GiB | lineitem GiB | vs dict/snappy |
|---|---|---|---|---|
| shuffle-dict-snappy (reference) | 518 s | 35.4 | 22.9 | 1.00x |
| shuffle-plain-snappy | 506 s | 45.5 | 32.0 | 1.28x |
| shuffle-delta-snappy | 500 s | 31.8 | 21.1 | 0.90x |
| shuffle-dict-zstd | 537 s | 24.8 | 16.5 | 0.70x |
| shuffle-plain-zstd | 493 s | 27.6 | 19.0 | 0.78x |
| shuffle-delta-zstd | 503 s | 25.7 | 17.6 | 0.72x |
| shuffle-dict-lz4raw | 546 s | 35.9 | 23.0 | 1.01x |
| shuffle-plain-lz4raw | 539 s | 47.9 | 33.8 | 1.35x |
| shuffle-delta-lz4raw | 519 s | 33.2 | 21.6 | 0.94x |
| keyorder-dict-snappy | 604 s | 33.7 | 21.6 | 0.95x |
| keyorder-plain-snappy | 592 s | 42.0 | 28.9 | 1.19x |
| keyorder-delta-snappy | 557 s | 29.3 | 19.4 | 0.83x |

## Reader support (SF1 smoke test, q1/q9/q13, all 12 variants x 4 engines)

Every engine reads every variant with identical result row counts (48 cells per
query agree), with one exception: **spark-rapids 26.04.2 hangs on
`shuffle-delta-zstd`** -- q9 spins at 100% GPU with the whole 80 GB allocated on
a 1 GB dataset, reproducibly (killed by the per-query timeout added to
`rapids/run_tpch_safe.sh` / `polars/run_polars_gpu.sh`, `QUERY_TIMEOUT`).
delta+snappy and delta+lz4raw are fine in RAPIDS, and cudf-polars 26.6 / Sirius
read delta+zstd in ~1 s, so it is a decoder bug in the cudf bundled with that
RAPIDS release, not a data problem. A full 22-query RAPIDS pass over the SF1
delta+zstd variant (120 s cap) hangs on exactly the 11 queries that read
`supplier` or `nation` (q2 q5 q7 q8 q9 q10 q11 q15 q16 q20 q21) and passes the
11 that do not -- i.e. it trips on the very small delta+zstd pages of the small
tables. In the SF100 rounds those cells are recorded as `TIMEOUT` (RAPIDS cap
300 s on that variant, `RAPIDS_DELTA_ZSTD_TIMEOUT`).

## Results (SF100, 3 cold rounds, 2026-09-23/24)

Run 23:16 -> 17:13 UTC (~18 h; ~28 min per variant per round, of which ~22 min
is RAPIDS' 22 JVM launches). 3168 cells: 3148 OK with **identical result row
counts across all engines, variants and rounds**, 20 `TIMEOUT` = spark-rapids
on delta+zstd (q2 q5 q7 q8 q9 q10 q11 q20; set varies slightly per round).
Full per-query medians: `results/format_ablation_report.md`
(`results/format_ablation_report.py --sf 100 --sizes <fmt_root>` regenerates it
from `format_ablation.csv`).

Sum over q1-q22 of the per-query **median cold seconds over 3 rounds**; in parentheses the min..max of the three per-round totals (run-to-run spread). `[n FAIL]` = queries that timed out in at least one round, excluded from that cell.

| variant | duckdb_cpu | sirius | polars_gpu | rapids |
|---|---|---|---|---|
| shuffle-plain-snappy | 31.4 (31.2..32.5) | 38.4 (37.2..39.9) | 80.0 (78.6..81.8) | 663.8 (650.5..690.2) |
| shuffle-dict-snappy | 29.4 (28.7..36.7) | 35.0 (34.0..38.0) | 72.0 (71.3..73.3) | 665.8 (639.4..697.7) |
| shuffle-delta-snappy | 29.7 (28.7..39.0) | 31.8 (31.8..32.7) | 40.9 (40.8..41.5) | 682.1 (676.4..705.2) |
| shuffle-plain-zstd | 31.2 (30.7..31.7) | 34.5 (34.6..35.4) | 92.0 (91.0..94.9) | 657.2 (649.0..688.2) |
| shuffle-dict-zstd | 29.9 (28.1..31.2) | 32.6 (32.3..34.1) | 79.4 (79.6..80.5) | 676.8 (666.1..703.7) |
| shuffle-delta-zstd | 27.9 (27.1..28.9) | 30.9 (30.7..32.1) | 41.8 (41.6..43.6) | 332.5 (328.0..342.8) [8 FAIL] |
| shuffle-plain-lz4raw | 29.4 (28.7..29.9) | 39.7 (39.5..40.9) | 98.4 (98.3..99.4) | 700.9 (692.9..727.2) |
| shuffle-dict-lz4raw | 28.1 (28.0..28.6) | 36.0 (34.9..36.9) | 85.0 (84.0..86.9) | 678.8 (665.4..685.0) |
| shuffle-delta-lz4raw | 28.0 (27.7..29.4) | 31.4 (31.4..32.9) | 43.0 (42.8..44.5) | 646.3 (633.9..683.7) |
| keyorder-plain-snappy | 27.7 (25.8..28.8) | 36.0 (35.6..36.5) | 70.9 (70.6..72.1) | 643.0 (635.7..671.4) |
| keyorder-dict-snappy | 26.4 (27.0..27.7) | 34.6 (34.4..35.4) | 59.6 (58.8..61.2) | 655.6 (630.2..680.0) |
| keyorder-delta-snappy | 24.9 (24.7..25.6) | 29.9 (29.7..31.4) | 45.4 (45.8..46.4) | 681.9 (653.3..692.0) |

## duckdb_cpu: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 31.4 (1.07x) | 31.2 (1.06x) | 29.4 (1.00x) |
| dict | 29.4 (1.00x) | 29.9 (1.02x) | 28.1 (0.96x) |
| delta | 29.7 (1.01x) | 27.9 (0.95x) | 28.0 (0.95x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 27.7 | 0.88x |
| dict | 26.4 | 0.90x |
| delta | 24.9 | 0.84x |

## sirius: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 38.4 (1.10x) | 34.5 (0.99x) | 39.7 (1.14x) |
| dict | 35.0 (1.00x) | 32.6 (0.93x) | 36.0 (1.03x) |
| delta | 31.8 (0.91x) | 30.9 (0.88x) | 31.4 (0.90x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 36.0 | 0.94x |
| dict | 34.6 | 0.99x |
| delta | 29.9 | 0.94x |

## polars_gpu: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 80.0 (1.11x) | 92.0 (1.28x) | 98.4 (1.37x) |
| dict | 72.0 (1.00x) | 79.4 (1.10x) | 85.0 (1.18x) |
| delta | 40.9 (0.57x) | 41.8 (0.58x) | 43.0 (0.60x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 70.9 | 0.89x |
| dict | 59.6 | 0.83x |
| delta | 45.4 | 1.11x |

## rapids: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 663.8 (1.00x) | 657.2 (0.99x) | 700.9 (1.05x) |
| dict | 665.8 (1.00x) | 676.8 (1.02x) | 678.8 (1.02x) |
| delta | 682.1 (1.02x) | 332.5 (0.50x) | 646.3 (0.97x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 643.0 | 0.97x |
| dict | 655.6 | 0.98x |
| delta | 681.9 | 1.00x |


### Findings

- **Encoding is the lever, and only for the GPU readers.** `delta` (page v2,
  DELTA_BINARY_PACKED/DELTA_BYTE_ARRAY) cuts cudf-polars to **0.57-0.60x** of the
  dict/snappy reference at every codec, and Sirius to **0.88-0.91x**. `plain` is
  the slowest for both (polars 1.11-1.37x, Sirius 1.10-1.14x). DuckDB is within
  +/-7% across all nine shuffle variants; RAPIDS is flat (+/-5%) because its
  per-query time is dominated by a fixed ~30 s/query cost, not the scan.
- **Codec matters little on ramdisk.** zstd vs snappy: Sirius -7%, DuckDB
  0-5% either way, polars_gpu +10% (decompression on the GPU costs more than
  the bytes it saves when the data is already in RAM). LZ4_RAW is never better
  than snappy for the GPU readers (polars +18%, Sirius +3%) and 4% better for
  DuckDB. zstd does buy 30% smaller files (24.8 vs 35.4 GiB).
- **Key order (rows in dbgen/primary-key order) is a mild, consistent win** for
  the reference encoding: DuckDB 0.84-0.90x, polars_gpu 0.83-0.89x (dict/plain),
  Sirius 0.94-0.99x, RAPIDS ~0.98x. The exception is polars_gpu on `delta`,
  where key order is 1.11x *slower* than the shuffled delta dataset.
- **Best format per engine (sum of medians):** DuckDB keyorder-delta-snappy
  (24.9 s, 0.85x ref); Sirius keyorder-delta-snappy (29.9 s, 0.85x); polars_gpu
  shuffle-delta-snappy (40.9 s, 0.57x); RAPIDS keyorder-plain-snappy (643 s,
  0.97x, within noise). The delta+zstd RAPIDS cell is not comparable (8 of 22
  queries time out).
- **Reader support caveat:** spark-rapids 26.04.2 cannot read delta+zstd
  reliably (hangs on the small-table pages, see above); everything else reads
  every variant.
- **Why polars_gpu moves 2x and Sirius ~10% (same cudf decoder).** A lineitem
  column-scan microbenchmark on ramdisk (min() over column sets, 2 passes)
  shows both engines read the delta files faster where it counts: the four
  high-cardinality key/price columns that fall back to PLAIN in the `dict`
  variant (l_orderkey/l_partkey/l_suppkey/l_extendedprice: 153 MB vs 87 MB per
  20 files) scan in 0.98 s (dict) vs 0.46 s (delta) on polars_gpu and 1.27 s vs
  0.75 s on Sirius, while DELTA_BYTE_ARRAY strings are *slower* (Sirius 0.69 s
  vs 0.93 s), so Sirius' all-column scan is a wash (1.9 s vs 2.0 s). The
  query-level gap is the executor, not the reader: polars_gpu runs the
  streaming executor with `target_partition_size` = 128 MB (`GPU_PART_MB`, sized
  for a 32 GB card at SF500), and its per-partition cost dominates -- q9 dict
  13.9 s vs delta 5.7 s at 128 MB partitions, but 3.3-3.7 s vs 1.8-3.5 s at
  1 GB partitions and 18 s vs 14-16 s with the in-memory executor. Smaller
  column chunks mean fewer partitions and less per-partition overhead, which
  amplifies any byte reduction. Sirius scans in 768 MiB batches
  (`scan_task_batch_size`), so it only sees the raw byte/decode difference
  (10-35% on the join-heavy queries, ~0 on string-heavy ones).
- Round-to-round spread is small (typically <3% of the total; one DuckDB
  dict/snappy round was +25% -- a shared-box noise event, absorbed by the
  median).

## Per-column size analysis (why the formats differ in bytes, and whether bytes predict time)

Data: `results/format_ablation_colsizes.csv` (compressed/encoded bytes and encodings per
variant x table x column, summed from every parquet footer by
`results/format_ablation_colsizes.py`). Tables: `results/format_ablation_columns.md` (also appended to `results/format_ablation_report.md`),
regenerated by `results/format_ablation_columns.py` (sections A-D). Summary:

- **Encoding movers.** Everything that moves is in lineitem. The four PLAIN-fallback
  INT64 columns (l_suppkey, l_orderkey, l_partkey, l_extendedprice, 2.8-3.1 GB each
  under plain/dict) drop to 0.53-0.60x with DELTA_BINARY_PACKED (about 1.25 GB saved
  each). The low-cardinality columns move the other way: dictionary takes
  l_shipinstruct / l_shipmode / l_linestatus / l_returnflag to 0.12-0.19x of plain,
  while DELTA_BYTE_ARRAY on the same columns is 3-10x *larger* than dictionary
  (l_shipinstruct 146 MB -> 1446 MB). Dates are a tie (dict 0.48x of plain, delta 0.50x).
  Comments (l_/o_/ps_comment, ~13.4 GB, 38% of the dict/snappy dataset) barely move
  with encoding (delta 0.92-0.97x) and are mostly not read by the queries.
- **Best encoding per column** (compressed, snappy): every high-cardinality number is
  best under delta (19/19), every dictionary-encodable column is best under dict
  (21/24; the 3 exceptions are ties within 5%), high-cardinality strings are best under
  delta by 5-10% (prefix sharing) except phones (plain). No column is best under PLAIN
  except those ties. A per-column mix would be 27.6 GiB vs the best uniform variant
  31.8 GiB (delta) / 35.4 GiB (dict).
- **Key order** only compresses the sort keys: l_orderkey delta 1474 -> 182 MB,
  o_orderkey 367 -> 5 MB, ps_partkey 149 -> 1 MB; dictionary also starts fitting for
  l_orderkey (0.53x). Whole dataset only 0.92-0.95x because comments dominate.
- **Codecs are not encoding-neutral.** Ratio compressed/encoded: plain 0.41 (snappy) /
  0.25 (zstd), dict 0.52 / 0.36, delta 0.53 / 0.42. The codec substitutes for the
  encoding: on dictionary-able columns plain+zstd reaches 0.16 but still lands at
  7.5 GiB vs 4.7 GiB for dict (which the codec cannot shrink further: 1.00 snappy,
  0.96 zstd). Delta output is the least compressible (bit-packed deltas), so
  delta+zstd (25.6 GiB) ends up larger than dict+zstd (24.8 GiB) although delta+snappy
  is smaller than dict+snappy. lz4raw ~= snappy (+3-5%). On the PLAIN-fallback keys
  zstd gains 0.64-0.67x over snappy; on comments 0.67x.
- **Size vs time.** Within a query, across the 12 variants, bytes of the touched columns
  predict median seconds for Sirius (mean r 0.78, 18/22 queries r>0.5) and polars_gpu
  (0.60, 17/22) but not DuckDB (0.30) or RAPIDS (0.05). Splitting the factor: Sirius
  responds to both codec (r 0.50) and encoding (0.75) bytes, i.e. it is byte-bound;
  polars_gpu responds only to encoding bytes (0.87) and not to codec bytes (-0.07), so
  zstd's smaller files do not help it (GPU zstd decompress and the partition-count
  effect dominate); DuckDB's CPU decompression cost cancels the byte saving (codec
  r -0.12; lz4 fastest); RAPIDS is flat (Spark overhead). Slopes: Sirius 0.34 s/GiB of
  dataset, polars_gpu 1.26 s/GiB (driven by the delta variants), DuckDB 0.05, RAPIDS 0.25
  but with r 0.11.
