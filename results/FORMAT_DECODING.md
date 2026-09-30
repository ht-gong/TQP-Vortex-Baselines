# Reading less vs. decoding less: dictionary and delta encoding in the SF100 format ablation

Companion to `FORMAT_ABLATION.md` (which measured *what* the parquet format does to
runtime). This report asks *why*, from the encoded bytes up: how much of a
dictionary a TPC-H query actually needs, how much of a delta stream can be
skipped, and what Sirius/Simpatico, Parquet+DuckDB and the wider state of the
art do about it -- separating throughout two different levers:

- **READ LESS** -- fewer bytes leave storage/RAM: projection, row-group/page/file
  skipping on statistics, dictionaries or bloom filters, clustering, late
  materialization of payload columns.
- **DECODE LESS** -- the bytes are read, but fewer of them are turned into
  values: predicates on dictionary codes, aggregating codes, dictionary
  vectors flowing through the executor, decode-and-filter fused kernels,
  random-access encodings.

All numbers are from the ablation's own SF100 datasets
(`/data/haotiang/parquet-ablation/fmt_sf100`, parquet-mr 1.13.1 via Spark
3.5.8; 1600 files x 1 row group per table). Tooling:
`results/format_ablation_pagestats.py` (parses page headers and encoded page
bodies straight from the files, no engine; output
`results/format_ablation_pagestats.csv`, 16 files per table) and
`results/format_ablation_decode.py` (`dict-hits`, `zonemap`, `micro-duckdb`,
`micro-gpu`, `tables`). Full tables T1-T4 are appended at the end.

## TL;DR

1. **Dictionary: the dictionary itself is never the cost.** Every
   dictionary-encoded TPC-H column has 2-2,546 entries per row group and the
   dictionary page is 0.02-1.8 % of the column chunk (T1); the other 98-99.98 %
   is the RLE/bit-packed **index stream** (1-12 bits per row, effectively no
   RLE runs in the shuffled layout). TPC-H filters touch 1/150 to 2/7 of the
   entries (1/150 p_type, 1/25 p_brand, 1/40 p_container, 1/7 l_shipmode, 1/3
   l_returnflag, 30/2526 l_shipdate for q14), group-bys touch all of them. So
   "skip decoding most of the dictionary" is trivially true and worth nothing;
   what *is* worth skipping is the **gather** (index -> value), which is 100 %
   avoidable for filter and group-by columns: DuckDB, which evaluates the
   predicate once on the dictionary and emits dictionary vectors, runs a
   filter on l_shipmode 2.4x and a group-by on it 5.7x faster from `dict`
   files than from `plain`; cudf (which materializes strings) gets 1.4-1.6x,
   purely from fewer bytes. The index stream cannot be skipped inside a page:
   no TPC-H predicate prunes a single row group or page in either layout
   (zone maps: 0 % of 1600 row groups for every query predicate), because
   matches are spread uniformly -- a 20k-row page is empty of matches with
   probability (1-s)^20000, i.e. only when selectivity s < 1e-4.
2. **Delta: nothing inside a page can be skipped, and the decode is per value.**
   DELTA_BINARY_PACKED stores one absolute value per page and a prefix-sum
   chain of 128-value blocks: value i needs every delta before it, so a
   filter cannot skip miniblocks and a reader cannot random-access. The work
   is the bit width (T3): 20-26 bits/value for the shuffled keys and prices,
   12-13 for dates (the same as the dictionary index width), 4-5 for
   discount/tax; width-0 miniblocks (free arithmetic progressions) occur only
   for constant columns (o_shippriority) and for dense sorted keys in the
   key-ordered layout (p_partkey, c_custkey 100 %; l_orderkey drops to 4.7
   bits, ps_partkey to 1 bit). DELTA_BYTE_ARRAY strings share only 15-25 % of
   their characters on the low-cardinality columns, so they are 3-10x larger
   than dictionary and decode sequentially -- 3-8x slower than `dict` in
   DuckDB and 2x slower in cudf. Delta's whole-query win for the GPU engines
   in the ablation was bytes (0.53-0.60x on the PLAIN-fallback keys) and
   partition-count effects, not decode skipping; in an isolated cudf scan
   `max(l_orderkey)` is 1.3x *slower* from delta than from PLAIN. Page-level
   skipping via the page index (present in these files) would work for delta
   exactly as for any other encoding, but only when rows are clustered on the
   predicate column: a 1 % key range prunes 98.9 % of row groups in key order
   (87.5 % even in "shuffle", because each dbgen batch is a key range), while
   the date predicates the queries actually use prune 0 %.
3. **What engines do.** *Sirius* decodes parquet with libcudf's standard
   `read_parquet` and does **read-less** work only at row-group granularity
   (column projection, min/max and null-count pruning, an AST row filter
   pushed to cuDF); it never sees parquet's dictionary/RLE/delta pages, uses
   neither the page index nor dictionary/bloom pruning, and its late
   materialization applies only to GPU-pinned tables. *Simpatico* is its own
   GPU compression library for **pinned** tables (re-encoded after the parquet
   decode with per-column plans: dictionary, bitpack, FOR, delta, RLE, ALP,
   FSST ...) whose JIT-fused decode can answer string `=`/`IN` off the key set
   without gathering and, behind an experimental flag, drop rows during
   bit-unpacking -- i.e. **decode less**, but on its own encoding, not on
   parquet's. *Parquet+DuckDB*: the format offers projection, chunk min/max,
   page index, bloom filters and dictionary pages as read-less hooks; DuckDB
   1.5 uses chunk statistics and byte-skips pages whose rows are already
   filtered but does not read the page index; on the decode-less side it
   evaluates table filters once per dictionary and emits dictionary vectors
   (PR #16136, Feb 2025), which is exactly what the microbenchmark shows.
   *cudf/cudf-polars*: the standard reader prunes row groups on statistics
   and bloom filters and applies the row filter after decoding; the
   experimental `hybrid_scan_reader` adds dictionary-page and page-index
   pruning plus a two-pass filter-columns-then-payload read with a data-page
   mask ("skip decompression and decoding of the masked pages"), which none
   of the engines here use yet. Section 5 lists the general state of the art
   in the two columns.

## 1. What the bytes look like

Sampled 16 of the 1600 lineitem files (one row group each, ~375k rows,
19 data pages per column chunk of ~19.7k rows). Every file carries a Parquet
**page index** (ColumnIndex + OffsetIndex per column chunk, T5) and **no**
bloom filters or page-header statistics.

### 1.1 Dictionary (`shuffle-dict-snappy`)

| column | entries / row group | dictionary bytes / row group | dictionary share of chunk | index bits / value | values in RLE runs |
|---|---|---|---|---|---|
| l_linestatus | 2 | 12 | 0.02 % | 1 | 0.9 % |
| l_returnflag | 3 | 17 | 0.02 % | 2 | 0.5 % |
| l_shipinstruct | 4 | 67 | 0.07 % | 2 | 0.0 % |
| l_shipmode | 7 | 58 | 0.04 % | 3 | 0.0 % |
| l_linenumber | 7 | 30 | 0.02 % | 3 | 0.0 % |
| l_discount / l_tax | 11 / 9 | 54 / 46 | 0.03 % | 4 | 0.0 % |
| l_quantity | 50 | 235 | 0.08 % | 6 | 0.0 % |
| l_shipdate / commit / receipt | 2,525 / 2,466 / 2,546 | ~10,100 | 1.8 % | 12 | 0.0 % |
| o_orderdate | 2,406 | 9,630 | 6.4 % | 12 | 0.0 % |
| o_orderpriority / o_orderstatus | 5 / 3 | 64 / 17 | 0.2 / 0.07 % | 3 / 2 | 0.0 / 0.7 % |
| p_type / p_brand / p_container / p_size | 150 / 25 / 40 / 50 | 1,148 / 121 / 264 / 204 | 8.4 / 1.5 / 2.7 / 2.1 % | 8 / 5 / 6 / 6 | 0 % |
| c_mktsegment / c_nationkey | 5 / 25 | 66 / 111 | 1.8 % | 3 / 5 | 0 % |
| ps_availqty | 9,931 | 39,732 | 31 % | 14 | 0 % |
| l_orderkey, l_partkey, l_suppkey, l_extendedprice, all comments, o_clerk, c_name/address/phone, ... | PLAIN fallback (dictionary exceeded parquet-mr's 1 MB cap) | | | | |

Two consequences. (a) Each row group's dictionary is the *whole* value domain
(every ship mode, every date...), so dictionary-based row-group pruning
("does any entry satisfy the predicate?") can never skip a TPC-H row group.
(b) With rows shuffled, the RLE/bit-packed hybrid degenerates to pure
bit-packing: <1 % of values sit in runs, so there is no run-level shortcut
either. In the key-ordered layout only the two columns correlated with
order key change: l_returnflag 20 % and l_linestatus 46 % of values in runs
(T2); l_orderkey itself becomes dictionary-encodable (93,855 entries, 16
bits) and o_shippriority is one 100 %-RLE run in both layouts.

### 1.2 Delta (`shuffle-delta-snappy` vs `keyorder-delta-snappy`)

| column | bits / value, shuffle | bits / value, keyorder | width-0 miniblocks | note |
|---|---|---|---|---|
| l_orderkey | 20.1 | **4.7** | 0 % | sorted: deltas 0 (same order) or small gaps |
| o_orderkey | 20.1 | **5.0** | 0 % | dbgen leaves gaps in the key space |
| ps_partkey | 15.2 | **1.0** | 0 % | 4 rows per key |
| p_partkey, c_custkey | 16.7 / 16.8 | **0.0** | **100 %** | dense 1..N: pure arithmetic progression, zero unpack work |
| l_partkey / l_suppkey | 25.5 / 21.0 | same | 0 % | random in both layouts |
| l_extendedprice / o_totalprice | 24.1 / 26.1 | same | 0 % | random |
| l_shipdate / commit / receipt / o_orderdate | 12.4-12.5 | 12.1-12.4 | 0 % | = the dictionary index width |
| l_quantity | 13.7 | same | 0 % | (dict: 6 bits) |
| l_discount / l_tax / l_linenumber | 5.0 / 4.3 / 4.0 | 5.0 / 4.3 / 3.0 | 0 % | (dict: 4 / 4 / 3 bits) |
| o_shippriority | 0 | 0 | 100 % | constant |

DELTA_BYTE_ARRAY (T4): shared prefix is 25 % of the characters for
l_shipinstruct, 15 % for l_shipmode, 20 % for o_orderpriority /
c_mktsegment, 8 % for p_type, ~0 % for comments; only p_mfgr (94 %), p_brand
(78 %), o_clerk (67 %) and c_name (74 %, 94 % in key order) share most of
their bytes. That is why delta strings are 3-10x larger than dictionary on
the low-cardinality columns (`FORMAT_ABLATION.md`, table A).

## 2. Q1 -- dictionary encoding: how much of the dictionary is queried?

### 2.1 Entries touched vs rows touched

`format_ablation_decode.py dict-hits` over the full SF100 tables (a row
group's dictionary is the whole domain, so table-level distinct counts are the
per-row-group dictionary sizes):

| query | column | predicate | dict entries | entries satisfying | rows passing |
|---|---|---|---|---|---|
| q1 | l_shipdate | <= 1998-09-02 | 2,526 | 2,436 (96 %) | 98.6 % |
| q1 | l_returnflag, l_linestatus | group by | 3, 2 | all | 100 % |
| q1 | l_quantity, l_discount, l_tax | sum / avg | 50, 11, 9 | all | 98.6 % |
| q3 | l_shipdate | > 1995-03-15 | 2,526 | 1,357 (54 %) | 53.9 % |
| q3 | c_mktsegment | = BUILDING | 5 | 1 | 20.0 % |
| q4 / q10 | o_orderdate | one quarter | 2,406 | 92 (3.8 %) | 3.8 % |
| q4 / q21 | l_commitdate vs l_receiptdate | column vs column | 2,466 | all (no code-level shortcut) | 63.2 % |
| q5 / q20 | o_orderdate / l_shipdate | one year | 2,406 / 2,526 | 365 (15 %) | 15.2 % |
| q6 | l_shipdate, l_discount, l_quantity | 1994, .05-.07, < 24 | 2,526, 11, 50 | 365, 3, 23 | 15.2 %, 27.3 %, 46.0 %; conjunction **1.90 %** |
| q7 / q8 | l_shipdate / o_orderdate | two years | 2,526 | 731 (29 %) | 30.4 % |
| q8 | p_type | = ECONOMY ANODIZED STEEL | 150 | 1 | 0.67 % |
| q10 | l_returnflag | = R | 3 | 1 | 24.7 % |
| q12 | l_shipmode | IN (MAIL, SHIP) | 7 | 2 | 28.6 %; with the date/column predicates **0.52 %** |
| q12 | o_orderpriority | IN (1-URGENT, 2-HIGH) (CASE) | 5 | 2 | 40.0 % |
| q14 | l_shipdate | one month | 2,526 | 30 (1.2 %) | **1.25 %** |
| q14 | p_type | LIKE PROMO% | 150 | 25 | 16.7 % |
| q15 | l_shipdate | one quarter | 2,526 | 91 (3.6 %) | 3.78 % |
| q16 | p_brand, p_type, p_size | <>, NOT LIKE, IN 8 | 25, 150, 50 | 24, 145, 8 | conjunction 14.9 % |
| q17 | p_brand, p_container | = Brand#23, = MED BOX | 25, 40 | 1, 1 | 4.0 %, 2.5 %; conjunction 0.10 % |
| q19 | l_shipmode, l_shipinstruct, l_quantity | IN (AIR, AIR REG), = DELIVER IN PERSON, 1..30 | 7, 4, 50 | 1, 1, 30 | 14.3 %, 25.0 %, 60 %; lineitem conjunction **2.14 %** |
| q19 | p_brand, p_container, p_size | three disjuncts | 25, 40, 50 | 3, 12, 15 | part disjunction 0.24 % |
| q21 | o_orderstatus | = F | 3 | 1 | 48.7 % |
| q2 | p_size, p_type | = 15, LIKE %BRASS | 50, 150 | 1, 30 | 2.0 %, 20 % |

So for **filter columns** a query needs 0.7-40 % of the dictionary entries
(one entry for the equality predicates, 30-365 of 2,526 dates for the
month/quarter/year ranges); for **group-by / aggregate columns** it needs all
of them; for the two column-vs-column predicates (q4, q12, q21) codes do not
help at all because the two columns have different dictionaries.

### 2.2 What can actually be skipped

Decoding a dictionary-encoded column chunk has three parts:

| step | cost in these files | skippable? |
|---|---|---|
| decode the dictionary page | 12 B - 10 KB per row group (0.02-1.8 % of the chunk); done once per chunk | irrelevant; it is also the step a reader must do to evaluate the predicate on codes |
| unpack the index stream | 1-12 bits x every row (375k rows = 47 KB - 563 KB per chunk); pure bit-packing, no runs | **no**, not within a page. Only whole pages/row groups can be skipped, on statistics -- and for TPC-H none can (section 2.3) |
| gather index -> value (materialize strings / dates / decimals) | the expensive step for strings: offsets + character copies | **yes, entirely** for filter columns (compare codes) and group-by columns (aggregate on codes, translate at the end); for payload columns only for the surviving rows |

The measurable payoff is the third row. `format_ablation_decode.py
micro-duckdb` / `micro-gpu` (SF100 lineitem, page-cached NVMe, best of 3;
`dict` / `plain` / `delta` are the three shuffled snappy variants; for
l_extendedprice and l_orderkey `dict` == PLAIN fallback):

| scan | DuckDB dict | plain | delta | cudf-polars dict | plain | delta |
|---|---|---|---|---|---|---|
| A filter on dictionary string: `count(*) where l_shipmode in (MAIL,SHIP)` | **0.19** | 0.44 (2.4x) | 0.58 (3.1x) | **0.17** | 0.28 (1.6x) | 0.36 (2.1x) |
| B group by dictionary string: `l_shipmode, count(*)` | **0.09** | 0.50 (5.7x) | 0.65 (7.4x) | **0.21** | 0.30 (1.4x) | 0.38 (1.8x) |
| C filter on dictionary int32: `count(*) where l_shipdate in 1994` | 0.09 | 0.12 (1.4x) | 0.09 | 0.31 | 0.31 | 0.26 |
| D payload after dictionary filter: `sum(l_extendedprice) where l_shipmode in (MAIL,SHIP)` | **0.31** | 0.55 (1.8x) | 0.65 (2.1x) | 0.39 | 0.45 | 0.45 |
| D0 payload, no filter: `sum(l_extendedprice)` | 0.15 | 0.13 | 0.09 | 0.31 | 0.33 | 0.28 |
| D1 payload after a 1.25 %-selective filter: `sum(l_extendedprice) where l_shipdate in 1995-09` | 0.18 | 0.22 | 0.13 | 0.43 | 0.50 | 0.43 |
| E high-cardinality int: `max(l_orderkey)` | 0.14 | 0.14 | **0.09** | 0.31 | 0.33 | 0.41 (1.3x) |
| F materialize dictionary string: `max(length(l_shipinstruct))` | **0.08** | 0.46 (5.6x) | 0.65 (8.0x) | **0.13** | 0.19 (1.5x) | 0.26 (2.0x) |
| G q6 (three dictionary filters, two payload columns) | 0.27 | 0.37 | 0.24 | 0.46 | 0.61 | 0.43 |

Reading:

- **DuckDB skips the gather and works on codes.** Its `DictionaryDecoder`
  evaluates the table filter once per dictionary (`filter_result[]` per
  entry) and emits a DuckDB dictionary vector (`result.Dictionary(dictionary,
  selection, count)`), so the filter (A), the hash aggregate on l_shipmode (B)
  and even `length()` (F) run on 3-bit codes; the 5-8x gap to `plain`/`delta`
  is the cost of materializing 600 M short strings. Note that B is faster than
  A: the group-by never needs anything but the code histogram.
- **cudf decodes dictionary pages to plain columns, so the dictionary helps
  only through bytes.** A/B/F gain 1.4-1.6x over plain, in line with the
  column being 6-9x smaller and the decoder being byte-bound; no code-level
  execution happens (no dictionary vectors exist in cudf's column model).
- **Nobody reads less for the payload column.** In both engines D1 (1.25 %
  of rows survive) costs *more* than D0 (no filter) on the dictionary files:
  the filter column is decoded in full and l_extendedprice is still read and
  decoded in full, because every 20k-row page contains matches. DuckDB does
  byte-skip pages whose rows are all filtered out (`PageIsFilteredOut` ->
  `Skip(compressed_page_size)`), but no page qualifies. This is the
  layout's fault, not the format's: see 2.3.
- **Dates are a wash between dict and delta** (C, D1): both are 12-bit
  streams; DuckDB's delta-int decoder is as fast as its dictionary path, cudf's
  a little faster.

### 2.3 Why no page or row group is skipped in TPC-H

`format_ablation_decode.py zonemap` over the 1600 row groups of each table,
min/max from the footers:

| predicate | shuffle: prunable row groups | keyorder: prunable |
|---|---|---|
| every query predicate on l_shipdate, l_receiptdate, o_orderdate, l_discount, l_quantity, l_shipmode, l_returnflag, l_shipinstruct, p_brand, p_container, p_size | **0.0 %** | **0.0 %** |
| hypothetical `l_orderkey` / `o_orderkey` in a 1 % key range | 87.5 % | 98.9 % |
| hypothetical `ps_partkey` in a 1 % key range | 87.5 % | 98.9 % |
| hypothetical `l_partkey` in a 1 % key range | 0.0 % | 0.0 % |

dbgen assigns dates, ship modes, brands and so on independently of the
primary key, so sorting on the key leaves every row group covering the whole
domain of every other column. Page granularity does not rescue this: with a
uniformly spread selectivity *s* the probability that a page of *n* rows has
no match is (1-s)^n; at n ~ 20k that is below 1e-40 for every TPC-H lineitem
predicate (s >= 0.52 %) and reaches 14 % only at s = 1e-4. The only pruning the
data supports is on the sort key -- and TPC-H never filters on it statically
(q18's `l_orderkey IN (...)` is a semi-join; join-key ranges could be pushed
as dynamic zone-map filters, which Sirius supports as an option but this
data would only reward on key-ordered files). The 87.5 % in "shuffle" is
because each of the 8 dbgen batches is a contiguous key range: the
round-robin shuffle is only within a batch, so files are 1/8-of-keyspace
clustered even there.

### 2.4 Answer

- Fraction of the **dictionary** a query needs: 1 entry to ~30 % of entries
  for filters, 100 % for group-by/aggregation columns. It is an irrelevant
  quantity: the dictionary is <2 % of the bytes and must be fully decoded to
  evaluate the predicate anyway.
- Fraction of the **index stream** that must be unpacked: 100 % in TPC-H. It
  could only drop through page/row-group pruning, which needs the rows
  clustered on the predicate column (date-sorted or Z-ordered data), not the
  key order tested here.
- Fraction of the **gather** that can be skipped: 100 % for filter and
  group-by columns, (1 - selectivity) for payload columns. This is what
  DuckDB already captures (2.4-5.7x on the string scans) and what cudf-based
  readers (Sirius, polars_gpu, RAPIDS) leave on the table, because libcudf
  materializes plain columns; on the GPU the equivalent lever is a
  decode-and-filter kernel over the codes, which is what Simpatico does for
  its own dictionary blobs (section 4).

## 3. Q2 -- delta encoding: how much of the delta decoding can be skipped?

### 3.1 Structure

A DELTA_BINARY_PACKED page = header (block size 128, 4 miniblocks of 32, total
count, **first value**) + blocks of (min_delta, 4 bit widths, 4 bit-packed
miniblocks). Only the first value of the *page* is absolute; every other
value is `first + sum(deltas before it)`. Therefore:

- **No random access and no skipping inside a page.** To produce value *i* the
  decoder must sum all *i-1* preceding deltas; to skip a block it still has
  to read and add every delta in it (a horizontal sum instead of a prefix
  scan -- cheaper, but every bit is still touched). DuckDB's
  `DeltaBinaryPackedDecoder::Skip` decodes the skipped values; cudf's decoder
  runs a block-wide prefix scan per miniblock. A predicate on the column
  cannot avoid unpacking any miniblock, unlike a dictionary predicate which
  is decided per code.
- **The only free case is a width-0 miniblock**: 32 values that are an
  arithmetic progression (`min_delta` apart) need no unpacking. T3 shows this
  happens for constant columns (o_shippriority) and, in the key-ordered
  layout, for the dense primary keys p_partkey and c_custkey (100 % of
  miniblocks); l_orderkey in key order is not free (4.7 bits: runs of equal
  keys interleaved with gaps), ps_partkey needs 1 bit.
- **Page-level skipping** works exactly as for any encoding -- the writer put a
  page index in these files -- but section 2.3 applies: 0 % of pages for any
  TPC-H predicate; 98.9 % for a key-range predicate on sorted keys.
- **DELTA_BYTE_ARRAY** is worse: each string is (prefix length, suffix), and
  the prefix refers to the *previous decoded string*, so decoding is a serial
  chain within a page; there is no code to compare against, so predicates
  need the full string. With 15-25 % prefix sharing on the low-cardinality
  columns the format also stores 3-10x more bytes than dictionary.

### 3.2 What the measurements say

- **Work is proportional to bit width, not to the number of values.** The
  shuffled key/price columns cost 20-26 bits/value, dates 12-13, discount/tax
  4-5 (T3). Delta on dates costs the same 12 bits/value as the dictionary
  index (and 1.05x the bytes); delta on keys halves the bytes of PLAIN
  (0.53-0.60x) because 20-26 bits < 64.
- **CPU: fewer bytes wins.** DuckDB decodes `max(l_orderkey)` from delta in
  0.09 s vs 0.14 s PLAIN (E), and q6 in 0.24 s vs 0.27 s (G): its delta
  decoder is cheap enough that halving the bytes pays even from page cache.
- **GPU: the per-value decode is the bottleneck.** cudf's isolated
  `max(l_orderkey)` is 0.41 s from delta vs 0.31-0.33 s from PLAIN (E, 1.3x
  *slower*), and `sum(l_extendedprice)` is a wash (0.28 vs 0.31-0.33). The
  block-serial prefix sum does not map onto a GPU as well as PLAIN's
  coalesced loads. The 0.57x total that `delta` bought polars_gpu in
  `FORMAT_ABLATION.md` therefore came from the streaming executor
  (smaller column chunks -> fewer 128 MB partitions -> less per-partition
  overhead, as that report established) and from bytes on Sirius (0.88-0.91x),
  not from decoding less.
- **Strings under delta lose everywhere**: A/B/F in DuckDB 3-8x slower than
  dictionary, 1.3-1.5x slower than PLAIN; cudf 2x slower than dictionary
  (table in 2.2).

### 3.3 Answer

Within a page: **0 %** of a DELTA_BINARY_PACKED stream can be skipped by a
predicate, and the reader must add every delta; only width-0 miniblocks are
free, which in this data means dense sorted keys (p_partkey, c_custkey in
key order) and constants. Across pages: everything the page index allows,
which for TPC-H predicates is again 0 % because rows are not clustered on the
filtered columns. Delta is a **read-less** encoding (half the bytes of PLAIN
on 64-bit keys) with a **decode-more** cost on GPUs; if the goal is to decode
less, the state of the art replaces it with layouts that restart the base
every vector (FastLanes-style FOR/delta over 1024 values, section 5) so that
a vector can be skipped or decoded in isolation and in parallel.

## 4. Q3a -- what Sirius and Simpatico do

Source: the Sirius clone at `sirius/sirius` (upstream `sirius-db/sirius`,
HEAD d79d4f97, libcudf 26.08), `docs/super-sirius/{scan,late-materialization,
compressed-pinning,dynamic-filters,optimizations}.md`.

**Parquet path (`src/op/scan/parquet_gpu_ingestible.cpp`).** Sirius has no
page decoder of its own; every split is one `cudf::io::read_parquet(sources,
metadatas, opts)` call (`:1152`). Around it:

| lever | done? | where |
|---|---|---|
| READ LESS: column projection (output + pure-filter columns; `count(*)` reads one narrow carrier column) | yes | `set_column_names`, `:598-605`; scan.md:139-160 |
| READ LESS: row-group pruning on footer min/max (stats-safe conjuncts only) and on null counts | yes | `hybrid_scan_reader::filter_row_groups_with_stats`, `:835-842`, `:844-922` |
| READ LESS: only surviving row groups read; splits packed to ~2.5 % of GPU memory of *decoded* bytes (`scan_task_batch_size`, 768 MiB in this repo's config) | yes | `set_row_groups`, `:1097-1102`; coalescer `:240-405` |
| READ LESS: dynamic join zone-map filters merged into the reader AST | optional, default off | `merge_dynamic_filters_into_ast`, `:1130-1147`; `enable_dynamic_zone_map_filter` |
| READ LESS: page index / page-level skipping, dictionary-page pruning, bloom-filter pruning, two-pass filter-then-payload read | **no** -- the hybrid-scan functions exist in the installed cuDF headers but are not called | grep of `src/` |
| row filter pushed into cuDF (`set_filter`) | yes, but cuDF applies it after decoding the projected columns (consistent with D1 above) | `:1104-1149`; cudf `parquet.hpp:465` |
| DECODE LESS on parquet encodings | **no**: cuDF returns plain columns; parquet dictionary/RLE/delta pages are never visible to Sirius | -- |
| late materialization (`src/late_mat/`) | GPU-**pinned** tables only, env-gated (`SIRIUS_EXP_LATE_MAT`); replaces wide columns by a row id and gathers them back at the end. It saves *carrying*, not reading or decoding: "A non-pinned scan cannot defer at all" (late-materialization.md:184-188) | |

**Simpatico (`src/compression/`, `simpatico_codegen/`).** A GPU compression
library with its own `.hpln` format, used when a table is **pinned** with
compression (`pin_table(..., compression => true)`; off by default,
`enable_pin_table_compression`). The parquet data is first decoded by cuDF to
a plain table and then re-encoded with a hand-written per-column plan
(`compress_with_plan(table_view, plan_dsl)`), e.g. lineitem strings ->
`dictionary -> keys, indices -> bitpack`; codecs: delta, RLE, bitpack, FOR,
zigzag, dictionary, ALP/ALP_RD, FSST-like `str_split`, plus nvcomp
byte-codecs. Fused compress/decompress kernels are generated as CUDA C++ per
plan shape and compiled with NVRTC at runtime. What it does with the encoding:

- **DECODE LESS, dictionary:** a string column consumed only by an `=`/`IN`
  filter is answered off the key set -- the predicate is resolved on the
  dictionary keys and mapped over the codes, "so the key chars are never
  gathered" (`simpatico_codegen.hpp:206-210`; `compressed_scan.cpp:281-297`);
  projected survivors gather only surviving keys. This is on whenever a
  compressed pin is scanned.
- **DECODE LESS, bit-packed numbers:** `SIRIUS_EXP_FUSED_SCAN_FILTER=1`
  (default off) evaluates range predicates during unpacking, writes survivors
  compacted, "rejected rows are never unpacked" (compressed-pinning.md:105-111),
  with selectivity caps of 10-35 % beyond which it falls back to full decode.
- **READ LESS:** projected columns' payload buffers only; pinned-chunk zone
  maps (`enable_pinned_zone_map_pruning`, default on).
- No join or aggregate consumes compressed data; everything is converted to
  plain cuDF columns before the next operator
  (`compression_converters.cpp`). A late-materialized column from a compressed
  pin "cannot skip its decode ... decompressed and then discarded"
  (late-materialization.md:171-172).

In this repo's Sirius runs (cold, `SIRIUS_ITERS=1`, parquet on ramdisk, no
`pin_table`) neither Simpatico nor late materialization is active; the
format effects in `FORMAT_ABLATION.md` are pure cuDF decode + bytes.

## 5. Q3b/c -- Parquet & DuckDB, and the state of the art, in two columns

### 5.1 Parquet + DuckDB

What the **format** offers (all present in the files here unless noted):

| hook | granularity | purpose | in these files |
|---|---|---|---|
| column chunks | column | projection (read less) | yes |
| `ColumnMetaData.statistics` min/max/null_count | row group | zone-map pruning (read less) | yes |
| Page index (ColumnIndex/OffsetIndex, format 2.5, parquet-mr >= 1.11) | page | page pruning + direct page offsets for late materialization (read less) | yes |
| Bloom filters (format 2.9, parquet-mr >= 1.12, opt-in) | row group | point-lookup pruning on high-cardinality keys (read less) | **no** |
| Dictionary page | row group | evaluate predicates on the domain: prune the chunk if no entry matches; compare codes instead of values (both) | yes |
| RLE_DICTIONARY indices, DELTA_*, PLAIN | page | the encodings themselves; RLE runs allow run-level evaluation, PLAIN allows random access, delta allows neither | yes |

What **DuckDB 1.5.5** does with them (`extension/parquet/`):

- READ LESS: projection; row-group zone maps; files/row groups skipped on
  filters pushed into the scan; pages whose rows are all already filtered are
  byte-skipped without decompression (`PageIsFilteredOut`,
  `trans.Skip(compressed_page_size)`); bloom filters are read since 1.2. It
  does **not** read the ColumnIndex/OffsetIndex, so page pruning happens only
  via the filter mask, not via page statistics.
- DECODE LESS: `DictionaryDecoder` materializes the dictionary once per chunk,
  evaluates table filters once on it (PR #16136, Feb 2025: "execute the
  filter once on the dictionary ... if the dictionary filters out all values,
  we don't even need to read the offset page and can skip it entirely") and
  emits **dictionary vectors** to the executor, so filters, `length()` and
  hash aggregation operate on codes (2.4x / 5.6x / 5.7x above). Delta
  decoders are streaming (PR #16105) but must decode to skip.

What **cudf-polars 26.6** (polars_gpu here) does: it converts the Polars
predicate to a cuDF AST and passes it with `set_filter` to the *standard*
`read_parquet` (`cudf_polars/dsl/ir.py:805-820`, and the rapidsmpf streaming
reader in `streaming/actor_graph/io.py`), plus column projection. That buys
row-group statistics/bloom pruning inside cuDF and a post-decode row filter;
no dictionary passthrough, no page index, no two-pass read. RAPIDS (Spark)
sits on the same libcudf reader.

### 5.2 State of the art, separated

**a. READ LESS -- fewer bytes**

| technique | representative work / system | what it needs from the data | applies to TPC-H here? |
|---|---|---|---|
| Projection, min/max zone maps (Small Materialized Aggregates) | Moerkotte VLDB 1998; every columnar engine | rows clustered on the predicate column | dates: no (0 %); sort key: yes |
| Page-level statistics & offsets | Parquet page index; Arrow C++/Rust, Impala, Trino, cuDF hybrid scan use it; DuckDB does not | same | same |
| Bloom filters | Parquet bloom filters; DuckDB 1.2, cuDF 25.02+, parquet-mr | point/IN predicates on high-cardinality keys | would help q18-style key semi-joins only |
| Dictionary-page pruning | parquet-mr `DictionaryFilter` (1.10), cuDF `filter_row_groups_with_dictionary_pages` | a row group whose dictionary lacks the value | never: every row group holds the full domain |
| Clustering / partitioning for skipping | Z-order & Hilbert clustering (Delta, Iceberg, Snowflake micro-partitions), "Fine-grained partitioning for aggressive data skipping" (Sun et al. SIGMOD 2014) | choose the sort key by workload (dates for TPC-H) | the missing ingredient: would turn 0 % into real pruning on q6/q14/q15 |
| Late materialization / two-pass scan | Abadi et al. ICDE 2007; cuDF `hybrid_scan_reader` (filter columns, then payload columns through a row mask and data-page mask); DuckDB filter-first with page byte-skip; Arrow-rs `RowSelection` | selective *and* clustered filters | selective yes (q6 1.9 %, q12 0.5 %, q14 1.25 %), clustered no -> payload pages still all touched |
| Sideways information passing / join filter pushdown | semi-join bloom/zone filters from the build side (DuckDB 1.2 join-filter pushdown, Sirius dynamic filters, Spark DPP, Velox) | key ranges or bloom on the probe column | Sirius: post-decode mask on parquet; range filters pay only on sorted keys |
| GPU-aware layout tuning of plain Parquet | "Do GPUs Really Need New Tabular File Formats?" (Luo, Chen, Binnig, DaMoN 2026): 125 GB/s from unmodified Parquet by choosing row-group/page sizes, codecs and encodings for the GPU | writer configuration | the same lever this ablation turned (delta vs dict, codec, sizes) |
| Random-access formats | FastLanes file format (VLDB 2025), Vortex (Spiral/LF AI), Lance, Nimble, BtrBlocks (SIGMOD 2023): 1024-value or per-chunk independent units so only needed vectors are fetched | new writer/reader | relevant to the TQP/Vortex baseline this repo feeds |

**b. DECODE LESS -- fewer bytes turned into values**

| technique | representative work / system | mechanism | status in the engines measured |
|---|---|---|---|
| Predicate on dictionary codes, aggregate on codes | Abadi, Madden, Ferreira "Integrating compression and execution in column-oriented database systems" (SIGMOD 2006); DuckDB dictionary vectors; Velox/Arrow/DataFusion dictionary arrays; Simpatico key-set answers | evaluate once per entry, then compare small integers; translate only output | DuckDB: yes (2.4-5.7x). cuDF-based (Sirius, polars_gpu, RAPIDS): no. Simpatico: yes, on its own dictionaries |
| SIMD / bit-level scans on packed codes | SIMD-Scan (Willhalm et al. VLDB 2009), BitWeaving (Li & Patel SIGMOD 2013), Data Blocks with positional SMAs (Lang et al. SIGMOD 2016) | scan the bit-packed stream directly, early-terminate per bit slice, skip blocks by local min/max | none of the parquet readers here; Simpatico's fused bitpack range filter is this on the GPU (experimental) |
| RLE-aware execution | Abadi 2006; Data Blocks; DuckDB's own storage | one comparison per run, aggregate run lengths | irrelevant here: <1 % of values in runs on shuffled data (20-46 % only on key-ordered flag columns) |
| Fused decode + filter (+ compaction) kernels | Simpatico JIT (`SIRIUS_EXP_FUSED_SCAN_FILTER`); cuDF hybrid scan data-page mask ("skip decompression and decoding of the masked pages"); FastLanes GPU decoding (DaMoN 2024) | the decoder consumes the predicate; rejected rows are never materialized | off by default in Sirius; not wired into Sirius/polars for parquet |
| Random-access / restartable lightweight encodings | FastLanes (VLDB 2023: unified transposed bit-packing, FOR/delta restarted per 1024 values, decoding with scalar code at >100 B ints/s; 2025 file format with a partial-decompression API returning compressed vectors to the engine); ALP for floats (SIGMOD 2024); Vortex compute-on-encoded kernels | a vector decodes independently and data-parallel, so a filter can skip vectors and a GPU thread block owns one | this is the answer to section 3: replaces parquet's page-serial delta and 1 MB-capped dictionaries |
| GPU string decoding on dictionaries / symbol tables | FSST (VLDB 2020) in Vortex/DuckDB; FastPair (arXiv 2609.15034, Sept 2026): reorganizes dictionary-code lookups for contiguous GPU writes, 1.6 TB/s, 2.4-4.2x over the B300 decompression engine | keep strings as codes as long as possible; make the final gather coalesced | cuDF materializes strings at decode; Sirius/Simpatico gathers only surviving keys |
| Compressed intermediate / operators on compressed data | Sirius "compressed materialization" (narrowed carriers, on by default), late materialization on pinned tables | fewer bytes carried through joins/aggregates | orthogonal to the scan; upstream measured late materialization at 7.39 -> 7.00 s over TPC-H SF1000 (gains only in q9/q10) |

### 5.3 Where this leaves the ablation's results

- The **GPU engines never decode less** than PLAIN would require: cuDF turns
  every page into a plain column, so the only format effect they can see is
  bytes (Sirius, byte-bound at 0.34 s/GiB) or partition counts (polars_gpu
  streaming). Delta wins there by 0.53-0.60x bytes on the four PLAIN-fallback
  key columns despite costing 1.3x more per value to decode.
- The **CPU engine decodes less** on dictionary columns thanks to code-level
  execution, which is why DuckDB is the only engine for which `dict` beats
  `plain` on string scans by 2-6x rather than 1.5x -- but at the whole-query
  level (`FORMAT_ABLATION.md`, +/-7 %) this is diluted by the PLAIN-fallback
  keys and payload decimals that dominate TPC-H bytes.
- **Nobody reads less in TPC-H** beyond projection, in either layout, because
  no query predicate is aligned with the physical order. Every read-less
  technique in 5.2a is idle on this data; the first experiment that would
  change that is a layout sorted (or Z-ordered) on the date columns, which
  would activate row-group pruning, page pruning and late materialization in
  every engine at once, and would also give RLE runs on the flag columns.
- For **decode less** on the GPU the levers are (i) dictionary-code predicate
  evaluation and dictionary passthrough in the reader (cuDF hybrid scan's
  dictionary stage + a code-level filter), (ii) fused decode-and-filter as in
  Simpatico, made to run on parquet's RLE_DICTIONARY pages rather than on a
  re-encoded pin, and (iii) for numbers, encodings with per-vector restarts
  instead of DELTA_BINARY_PACKED's page-serial chain.

## Appendix -- full tables (`format_ablation_decode.py tables`)

### T1. Dictionary anatomy, shuffle-dict-snappy (16 sampled files per table; one row group per file)

| column | type | dict pages/chunks | entries per chunk | dict bytes per chunk | dict share of chunk bytes | index bits/value | values in RLE runs | data pages/chunk |
|---|---|---|---|---|---|---|---|---|
| l_orderkey | INT64 | PLAIN fallback |  |  |  |  |  | 19 |
| l_partkey | INT64 | PLAIN fallback |  |  |  |  |  | 19 |
| l_suppkey | INT64 | PLAIN fallback |  |  |  |  |  | 19 |
| l_linenumber | INT32 | 16/16 | 7 | 30 | 0.02% | 3 | 0.0% | 19 |
| l_quantity | INT64 | 16/16 | 50 | 235 | 0.08% | 6 | 0.0% | 19 |
| l_extendedprice | INT64 | PLAIN fallback |  |  |  |  |  | 19 |
| l_discount | INT64 | 16/16 | 11 | 54 | 0.03% | 4 | 0.0% | 19 |
| l_tax | INT64 | 16/16 | 9 | 46 | 0.02% | 4 | 0.0% | 19 |
| l_returnflag | BYTE_ARRAY | 16/16 | 3 | 17 | 0.02% | 2 | 0.5% | 19 |
| l_linestatus | BYTE_ARRAY | 16/16 | 2 | 12 | 0.02% | 1 | 0.9% | 19 |
| l_shipdate | INT32 | 16/16 | 2,525 | 10,108 | 1.76% | 12 | 0.0% | 19 |
| l_commitdate | INT32 | 16/16 | 2,466 | 9,869 | 1.72% | 12 | 0.0% | 19 |
| l_receiptdate | INT32 | 16/16 | 2,546 | 10,188 | 1.77% | 12 | 0.0% | 19 |
| l_shipinstruct | BYTE_ARRAY | 16/16 | 4 | 67 | 0.07% | 2 | 0.0% | 19 |
| l_shipmode | BYTE_ARRAY | 16/16 | 7 | 58 | 0.04% | 3 | 0.0% | 19 |
| l_comment | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 19 |
| o_orderkey | INT64 | PLAIN fallback |  |  |  |  |  | 5 |
| o_custkey | INT64 | PLAIN fallback |  |  |  |  |  | 5 |
| o_orderstatus | BYTE_ARRAY | 16/16 | 3 | 17 | 0.07% | 2 | 0.7% | 5 |
| o_totalprice | INT64 | PLAIN fallback |  |  |  |  |  | 5 |
| o_orderdate | INT32 | 16/16 | 2,406 | 9,630 | 6.39% | 12 | 0.0% | 5 |
| o_orderpriority | BYTE_ARRAY | 16/16 | 5 | 64 | 0.18% | 3 | 0.0% | 5 |
| o_clerk | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 5 |
| o_shippriority | INT32 | 16/16 | 1 | 6 | 2.73% | 0 | 100.0% | 5 |
| o_comment | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 6 |
| ps_partkey | INT64 | PLAIN fallback |  |  |  |  |  | 3 |
| ps_suppkey | INT64 | PLAIN fallback |  |  |  |  |  | 3 |
| ps_availqty | INT32 | 16/16 | 9,931 | 39,732 | 31.16% | 14 | 0.0% | 3 |
| ps_supplycost | INT64 | PLAIN fallback |  |  |  |  |  | 3 |
| ps_comment | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 7 |
| p_partkey | INT64 | PLAIN fallback |  |  |  |  |  | 1 |
| p_name | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |
| p_mfgr | BYTE_ARRAY | 16/16 | 5 | 40 | 0.83% | 3 | 0.0% | 1 |
| p_brand | BYTE_ARRAY | 16/16 | 25 | 121 | 1.51% | 5 | 0.0% | 1 |
| p_type | BYTE_ARRAY | 16/16 | 150 | 1,148 | 8.35% | 8 | 0.0% | 1 |
| p_size | INT32 | 16/16 | 50 | 204 | 2.11% | 6 | 0.0% | 1 |
| p_container | BYTE_ARRAY | 16/16 | 40 | 264 | 2.71% | 6 | 0.0% | 1 |
| p_retailprice | INT64 | PLAIN fallback |  |  |  |  |  | 1 |
| p_comment | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |
| c_custkey | INT64 | PLAIN fallback |  |  |  |  |  | 1 |
| c_name | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |
| c_address | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |
| c_nationkey | INT64 | 16/16 | 25 | 111 | 1.83% | 5 | 0.0% | 1 |
| c_phone | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |
| c_acctbal | INT64 | PLAIN fallback |  |  |  |  |  | 1 |
| c_mktsegment | BYTE_ARRAY | 16/16 | 5 | 66 | 1.79% | 3 | 0.0% | 1 |
| c_comment | BYTE_ARRAY | PLAIN fallback |  |  |  |  |  | 1 |

### T2. keyorder-dict-snappy: columns whose dictionary anatomy changes

| column | dict pages/chunks | entries per chunk | index bits/value | values in RLE runs | chunk bytes vs shuffle |
|---|---|---|---|---|---|
| l_orderkey | 16/16 | 93,855 | 15.8 | 0.0% | 0.53x |
| l_linenumber | 16/16 | 7 | 3.0 | 0.0% | 0.71x |
| l_returnflag | 16/16 | 3 | 2.0 | 20.3% | 0.97x |
| l_linestatus | 16/16 | 2 | 1.0 | 45.6% | 1.21x |
| o_orderkey | 0/16 | PLAIN fallback |  | 0.0% | 0.82x |
| ps_partkey | 16/16 | 12,542 | 13.6 | 0.0% | 0.62x |
| ps_suppkey | 0/16 | PLAIN fallback |  | 0.0% | 0.88x |
| p_partkey | 0/16 | PLAIN fallback |  | 0.0% | 0.89x |
| p_retailprice | 16/16 | 2,173 | 12.0 | 0.0% | 0.50x |
| c_custkey | 0/16 | PLAIN fallback |  | 0.0% | 0.90x |
| c_name | 0/16 | PLAIN fallback |  | 0.0% | 0.78x |

### T3. DELTA_BINARY_PACKED: miniblock (32 values) bit widths, shuffle vs keyorder

| column | type | layout | mean bits/value | width-0 miniblocks | top widths |
|---|---|---|---|---|---|
| l_orderkey | INT64 | shuffle | 20.1 | 0% | 20b:94% 19b:4% 27b:1% |
| l_orderkey | INT64 | keyorder | 4.7 | 0% | 5b:93% 1b:7% 0b:0% |
| l_partkey | INT64 | shuffle | 25.5 | 0% | 26b:52% 25b:48% 24b:0% |
| l_partkey | INT64 | keyorder | 25.5 | 0% | 26b:52% 25b:48% 0b:0% |
| l_suppkey | INT64 | shuffle | 21.0 | 0% | 21b:100% 20b:0% 0b:0% |
| l_suppkey | INT64 | keyorder | 21.0 | 0% | 21b:100% 20b:0% 19b:0% |
| l_linenumber | INT32 | shuffle | 4.0 | 0% | 4b:100% 3b:0% 0b:0% |
| l_linenumber | INT32 | keyorder | 3.0 | 0% | 3b:100% 0b:0% 1b:0% |
| l_quantity | INT64 | shuffle | 13.7 | 0% | 14b:66% 13b:34% 0b:0% |
| l_quantity | INT64 | keyorder | 13.7 | 0% | 14b:66% 13b:34% 0b:0% |
| l_extendedprice | INT64 | shuffle | 24.1 | 0% | 24b:94% 25b:6% 23b:0% |
| l_extendedprice | INT64 | keyorder | 24.1 | 0% | 24b:94% 25b:6% 23b:0% |
| l_discount | INT64 | shuffle | 5.0 | 0% | 5b:97% 4b:3% 0b:0% |
| l_discount | INT64 | keyorder | 5.0 | 0% | 5b:97% 4b:3% 0b:0% |
| l_tax | INT64 | shuffle | 4.3 | 0% | 4b:72% 5b:28% 0b:0% |
| l_tax | INT64 | keyorder | 4.3 | 0% | 4b:73% 5b:27% 0b:0% |
| l_shipdate | INT32 | shuffle | 12.5 | 0% | 12b:54% 13b:46% 0b:0% |
| l_shipdate | INT32 | keyorder | 12.1 | 0% | 12b:91% 13b:8% 11b:1% |
| l_commitdate | INT32 | shuffle | 12.4 | 0% | 12b:55% 13b:45% 0b:0% |
| l_commitdate | INT32 | keyorder | 12.1 | 0% | 12b:92% 13b:8% 11b:1% |
| l_receiptdate | INT32 | shuffle | 12.5 | 0% | 12b:54% 13b:46% 0b:0% |
| l_receiptdate | INT32 | keyorder | 12.1 | 0% | 12b:91% 13b:8% 11b:1% |
| o_orderkey | INT64 | shuffle | 20.1 | 0% | 20b:95% 19b:2% 25b:1% |
| o_orderkey | INT64 | keyorder | 5.0 | 0% | 5b:100% 0b:0% 1b:0% |
| o_custkey | INT64 | shuffle | 25.0 | 0% | 25b:100% 24b:0% 0b:0% |
| o_custkey | INT64 | keyorder | 25.0 | 0% | 25b:100% 23b:0% 24b:0% |
| o_totalprice | INT64 | shuffle | 26.1 | 0% | 26b:94% 27b:6% 25b:0% |
| o_totalprice | INT64 | keyorder | 26.1 | 0% | 26b:94% 27b:6% 25b:0% |
| o_orderdate | INT32 | shuffle | 12.4 | 0% | 12b:55% 13b:45% 0b:0% |
| o_orderdate | INT32 | keyorder | 12.4 | 0% | 12b:56% 13b:44% 0b:0% |
| o_shippriority | INT32 | shuffle | 0.0 | 100% | 0b:100% 1b:0% 10b:0% |
| o_shippriority | INT32 | keyorder | 0.0 | 100% | 0b:100% 1b:0% 10b:0% |
| ps_partkey | INT64 | shuffle | 15.2 | 0% | 15b:92% 14b:2% 16b:2% |
| ps_partkey | INT64 | keyorder | 1.0 | 0% | 1b:100% 0b:0% 10b:0% |
| ps_suppkey | INT64 | shuffle | 21.0 | 0% | 21b:100% 20b:0% 0b:0% |
| ps_suppkey | INT64 | keyorder | 20.0 | 0% | 20b:100% 0b:0% 1b:0% |
| ps_availqty | INT32 | shuffle | 14.6 | 0% | 15b:64% 14b:36% 13b:0% |
| ps_availqty | INT32 | keyorder | 14.6 | 0% | 15b:64% 14b:36% 0b:0% |
| ps_supplycost | INT64 | shuffle | 18.0 | 0% | 18b:100% 17b:0% 0b:0% |
| ps_supplycost | INT64 | keyorder | 18.0 | 0% | 18b:100% 17b:0% 0b:0% |
| p_partkey | INT64 | shuffle | 16.7 | 0% | 16b:74% 17b:11% 20b:7% |
| p_partkey | INT64 | keyorder | 0.0 | 100% | 0b:100% 1b:0% 10b:0% |
| p_size | INT32 | shuffle | 7.0 | 0% | 7b:100% 6b:0% 0b:0% |
| p_size | INT32 | keyorder | 7.0 | 0% | 7b:100% 6b:0% 0b:0% |
| p_retailprice | INT64 | shuffle | 18.0 | 0% | 18b:100% 17b:0% 0b:0% |
| p_retailprice | INT64 | keyorder | 3.1 | 0% | 1b:87% 17b:13% 15b:0% |
| c_custkey | INT64 | shuffle | 16.8 | 0% | 16b:70% 17b:8% 20b:7% |
| c_custkey | INT64 | keyorder | 0.0 | 100% | 0b:100% 1b:0% 10b:0% |
| c_nationkey | INT64 | shuffle | 6.0 | 0% | 6b:100% 5b:0% 3b:0% |
| c_nationkey | INT64 | keyorder | 6.0 | 0% | 6b:100% 5b:0% 0b:0% |
| c_acctbal | INT64 | shuffle | 21.0 | 0% | 21b:98% 22b:2% 17b:0% |
| c_acctbal | INT64 | keyorder | 21.0 | 0% | 21b:98% 22b:2% 20b:0% |

### T4. DELTA_BYTE_ARRAY: shared prefix vs stored suffix (shuffle-delta-snappy)

| column | mean prefix chars (shared) | mean suffix chars (stored) | prefix share |
|---|---|---|---|
| l_returnflag | 0.4 | 0.6 | 38% |
| l_linestatus | 0.5 | 0.5 | 50% |
| l_shipinstruct | 3.0 | 9.0 | 25% |
| l_shipmode | 0.7 | 3.6 | 15% |
| l_comment | 0.1 | 26.4 | 0% |
| o_orderstatus | 0.5 | 0.5 | 47% |
| o_orderpriority | 1.7 | 6.7 | 20% |
| o_clerk | 10.1 | 4.9 | 67% |
| o_comment | 0.1 | 48.4 | 0% |
| ps_comment | 0.1 | 123.4 | 0% |
| p_name | 0.1 | 32.6 | 0% |
| p_mfgr | 13.2 | 0.8 | 94% |
| p_brand | 6.2 | 1.8 | 78% |
| p_type | 1.6 | 19.0 | 8% |
| p_container | 1.0 | 6.6 | 13% |
| p_comment | 0.1 | 13.4 | 1% |
| c_name | 13.4 | 4.6 | 74% |
| c_address | 0.0 | 25.0 | 0% |
| c_phone | 0.4 | 14.6 | 3% |
| c_mktsegment | 1.8 | 7.2 | 20% |
| c_comment | 0.1 | 72.4 | 0% |

