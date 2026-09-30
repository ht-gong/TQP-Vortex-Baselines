#!/usr/bin/env python3
"""Decode-vs-read analysis of the SF100 parquet-format ablation (results/FORMAT_DECODING.md).

  datagen-venv/bin/python results/format_ablation_decode.py dict-hits   # dictionary entries touched per query predicate + row selectivity
  datagen-venv/bin/python results/format_ablation_decode.py zonemap     # row-group min/max pruning potential, shuffle vs keyorder
  datagen-venv/bin/python results/format_ablation_decode.py micro-duckdb # DuckDB dict/plain/delta reader microbenchmark
  polars/.venv-gpu/bin/python results/format_ablation_decode.py micro-gpu    # same on cudf-polars (set CUDA_VISIBLE_DEVICES)
  datagen-venv/bin/python results/format_ablation_decode.py tables      # T1-T5 from results/format_ablation_pagestats.csv

Env: FMT_ROOT (default /data/haotiang/parquet-ablation/fmt_sf100). All output is markdown.
"""
import csv, os, sys, time
from collections import defaultdict
R = os.environ.get("FMT_ROOT", "/data/haotiang/parquet-ablation/fmt_sf100")
HERE = os.path.dirname(os.path.abspath(__file__))
V3 = ["shuffle-dict-snappy", "shuffle-plain-snappy", "shuffle-delta-snappy"]


def md(hdr, rows):
    print("| " + " | ".join(hdr) + " |"); print("|" + "---|" * len(hdr))
    for r in rows: print("| " + " | ".join(str(x) for x in r) + " |")
    print()


# ------------------------------------------------------------------ dict-hits
def dict_hits():
    import duckdb
    con = duckdb.connect(); con.execute("set threads=128")
    for t in ["lineitem", "orders", "part", "customer"]:
        con.execute(f"create view {t} as select * from read_parquet('{R}/shuffle-dict-snappy/parquet/{t}/*.parquet')")
    # (query, table, column, label, predicate SQL or None for group-by/aggregate columns)
    P = [
        ("q1", "lineitem", "l_shipdate", "<= 1998-09-02", "l_shipdate <= date '1998-09-02'"),
        ("q1", "lineitem", "l_returnflag", "group by", None), ("q1", "lineitem", "l_linestatus", "group by", None),
        ("q1", "lineitem", "l_quantity", "sum/avg", None), ("q1", "lineitem", "l_discount", "sum/avg", None), ("q1", "lineitem", "l_tax", "sum", None),
        ("q3", "lineitem", "l_shipdate", "> 1995-03-15", "l_shipdate > date '1995-03-15'"),
        ("q3", "customer", "c_mktsegment", "= BUILDING", "c_mktsegment = 'BUILDING'"),
        ("q3", "orders", "o_orderdate", "< 1995-03-15", "o_orderdate < date '1995-03-15'"),
        ("q4", "orders", "o_orderdate", "1993Q3", "o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'"),
        ("q4", "orders", "o_orderpriority", "group by", None),
        ("q4", "lineitem", "l_commitdate", "l_commitdate < l_receiptdate (col vs col)", "l_commitdate < l_receiptdate"),
        ("q5", "orders", "o_orderdate", "1994", "o_orderdate >= date '1994-01-01' and o_orderdate < date '1995-01-01'"),
        ("q6", "lineitem", "l_shipdate", "1994", "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'"),
        ("q6", "lineitem", "l_discount", "0.05..0.07", "l_discount between 0.05 and 0.07"),
        ("q6", "lineitem", "l_quantity", "< 24", "l_quantity < 24"),
        ("q6", "lineitem", "(all three)", "conjunction", "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01' and l_discount between 0.05 and 0.07 and l_quantity < 24"),
        ("q7", "lineitem", "l_shipdate", "1995-1996", "l_shipdate between date '1995-01-01' and date '1996-12-31'"),
        ("q8", "orders", "o_orderdate", "1995-1996", "o_orderdate between date '1995-01-01' and date '1996-12-31'"),
        ("q8", "part", "p_type", "= ECONOMY ANODIZED STEEL", "p_type = 'ECONOMY ANODIZED STEEL'"),
        ("q10", "orders", "o_orderdate", "1993Q4", "o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'"),
        ("q10", "lineitem", "l_returnflag", "= R", "l_returnflag = 'R'"),
        ("q12", "lineitem", "l_shipmode", "IN (MAIL, SHIP)", "l_shipmode in ('MAIL','SHIP')"),
        ("q12", "lineitem", "l_receiptdate", "1994", "l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'"),
        ("q12", "lineitem", "(all)", "conjunction", "l_shipmode in ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'"),
        ("q12", "orders", "o_orderpriority", "IN (1-URGENT, 2-HIGH) (CASE)", "o_orderpriority in ('1-URGENT','2-HIGH')"),
        ("q14", "lineitem", "l_shipdate", "1995-09", "l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-10-01'"),
        ("q14", "part", "p_type", "LIKE PROMO%", "p_type like 'PROMO%'"),
        ("q15", "lineitem", "l_shipdate", "1996Q1", "l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'"),
        ("q16", "part", "p_brand", "<> Brand#45", "p_brand <> 'Brand#45'"),
        ("q16", "part", "p_type", "NOT LIKE MEDIUM POLISHED%", "p_type not like 'MEDIUM POLISHED%'"),
        ("q16", "part", "p_size", "IN (8 values)", "p_size in (49,14,23,45,19,3,36,9)"),
        ("q16", "part", "(all)", "conjunction", "p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49,14,23,45,19,3,36,9)"),
        ("q17", "part", "p_brand", "= Brand#23", "p_brand = 'Brand#23'"),
        ("q17", "part", "p_container", "= MED BOX", "p_container = 'MED BOX'"),
        ("q17", "part", "(all)", "conjunction", "p_brand = 'Brand#23' and p_container = 'MED BOX'"),
        ("q19", "lineitem", "l_shipmode", "IN (AIR, AIR REG)", "l_shipmode in ('AIR','AIR REG')"),
        ("q19", "lineitem", "l_shipinstruct", "= DELIVER IN PERSON", "l_shipinstruct = 'DELIVER IN PERSON'"),
        ("q19", "lineitem", "l_quantity", "1..30 (union of 3 ranges)", "l_quantity between 1 and 30"),
        ("q19", "lineitem", "(lineitem-only)", "conjunction", "l_shipmode in ('AIR','AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' and l_quantity between 1 and 30"),
        ("q19", "part", "p_brand", "IN (3 brands)", "p_brand in ('Brand#12','Brand#23','Brand#34')"),
        ("q19", "part", "p_container", "IN (12 containers)", "p_container in ('SM CASE','SM BOX','SM PACK','SM PKG','MED BAG','MED BOX','MED PKG','MED PACK','LG CASE','LG BOX','LG PACK','LG PKG')"),
        ("q19", "part", "p_size", "1..15", "p_size between 1 and 15"),
        ("q19", "part", "(part-only)", "disjunction", "(p_brand='Brand#12' and p_container in ('SM CASE','SM BOX','SM PACK','SM PKG') and p_size between 1 and 5) or (p_brand='Brand#23' and p_container in ('MED BAG','MED BOX','MED PKG','MED PACK') and p_size between 1 and 10) or (p_brand='Brand#34' and p_container in ('LG CASE','LG BOX','LG PACK','LG PKG') and p_size between 1 and 15)"),
        ("q20", "lineitem", "l_shipdate", "1994", "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'"),
        ("q21", "orders", "o_orderstatus", "= F", "o_orderstatus = 'F'"),
        ("q21", "lineitem", "l_receiptdate", "l_receiptdate > l_commitdate (col vs col)", "l_receiptdate > l_commitdate"),
        ("q2", "part", "p_size", "= 15", "p_size = 15"),
        ("q2", "part", "p_type", "LIKE %BRASS", "p_type like '%BRASS'"),
    ]
    # one pass per table
    by_t = defaultdict(list)
    for p in P: by_t[p[1]].append(p)
    rows = []
    for t, ps in by_t.items():
        sel = ["count(*)"]
        for i, (q, _, c, lab, pred) in enumerate(ps):
            single = not c.startswith("(")
            sel.append(f"count(distinct {c})" if single else "NULL")
            if pred is None:
                sel += [f"count(distinct {c})", "count(*)"]
            else:
                sel += [f"count(distinct {c}) filter ({pred})" if single else "NULL", f"count(*) filter ({pred})"]
        r = con.execute(f"select {', '.join(sel)} from {t}").fetchone()
        n = r[0]
        for i, (q, _, c, lab, pred) in enumerate(ps):
            d, h, k = r[1 + 3 * i], r[2 + 3 * i], r[3 + 3 * i]
            rows.append((q, t, c, lab, d if d is not None else "", h if h is not None else "",
                         f"{100 * h / d:.1f}%" if (d and h is not None) else "", f"{100 * k / n:.2f}%"))
    rows.sort(key=lambda r: (int(r[0][1:]), r[1], r[2]))
    print("## Dictionary entries touched per predicate (SF100, shuffle-dict-snappy; entries = distinct values in the whole table = dictionary size, every row group holds all of them)\n")
    md(["query", "table", "column", "predicate", "dict entries", "entries satisfying predicate", "entry share", "rows passing"], rows)


# ------------------------------------------------------------------ zonemap
def zonemap():
    import duckdb
    con = duckdb.connect(); con.execute("set threads=64")
    preds = {
        'q1 l_shipdate <= 1998-09-02': ('lineitem', 'l_shipdate', "mn::date <= date '1998-09-02'"),
        'q3 l_shipdate > 1995-03-15': ('lineitem', 'l_shipdate', "mx::date > date '1995-03-15'"),
        'q6/q20 l_shipdate in 1994': ('lineitem', 'l_shipdate', "mx::date >= date '1994-01-01' and mn::date < date '1995-01-01'"),
        'q14 l_shipdate 1995-09': ('lineitem', 'l_shipdate', "mx::date >= date '1995-09-01' and mn::date < date '1995-10-01'"),
        'q15 l_shipdate 1996Q1': ('lineitem', 'l_shipdate', "mx::date >= date '1996-01-01' and mn::date < date '1996-04-01'"),
        'q12 l_receiptdate in 1994': ('lineitem', 'l_receiptdate', "mx::date >= date '1994-01-01' and mn::date < date '1995-01-01'"),
        'q4 o_orderdate 1993Q3': ('orders', 'o_orderdate', "mx::date >= date '1993-07-01' and mn::date < date '1993-10-01'"),
        'q10 o_orderdate 1993Q4': ('orders', 'o_orderdate', "mx::date >= date '1993-10-01' and mn::date < date '1994-01-01'"),
        'q6 l_discount .05-.07': ('lineitem', 'l_discount', "mx::decimal(11,2) >= 0.05 and mn::decimal(11,2) <= 0.07"),
        'q12 l_shipmode in MAIL,SHIP': ('lineitem', 'l_shipmode', "mn <= 'SHIP' and mx >= 'MAIL'"),
        'q19 l_shipinstruct = DELIVER IN PERSON': ('lineitem', 'l_shipinstruct', "mn <= 'DELIVER IN PERSON' and mx >= 'DELIVER IN PERSON'"),
        'q17 p_brand = Brand#23': ('part', 'p_brand', "mn <= 'Brand#23' and mx >= 'Brand#23'"),
        'q17 p_container = MED BOX': ('part', 'p_container', "mn <= 'MED BOX' and mx >= 'MED BOX'"),
        'q2 p_size = 15': ('part', 'p_size', "mn::int <= 15 and mx::int >= 15"),
        'hypothetical: l_orderkey in a 1% key range': ('lineitem', 'l_orderkey', "mx::bigint >= 100000000 and mn::bigint < 106000000"),
        'hypothetical: o_orderkey in a 1% key range': ('orders', 'o_orderkey', "mx::bigint >= 100000000 and mn::bigint < 106000000"),
        'hypothetical: l_partkey in a 1% key range': ('lineitem', 'l_partkey', "mx::bigint >= 1000000 and mn::bigint < 1200000"),
        'hypothetical: ps_partkey in a 1% key range': ('partsupp', 'ps_partkey', "mx::bigint >= 1000000 and mn::bigint < 1200000"),
    }
    print("## Row-group zone maps: share of the 1600 row groups a predicate could skip on min/max (dict-snappy)\n")
    rows = []
    for name, (t, c, cond) in preds.items():
        cells = []
        for v in ['shuffle-dict-snappy', 'keyorder-dict-snappy']:
            n, k = con.execute(f"select count(*), count(*) filter ({cond}) from parquet_metadata('{R}/{v}/parquet/{t}/*.parquet') "
                               f"where path_in_schema='{c}'").fetchone()
            cells.append(f"{100 * (1 - k / n):.1f}%")
        rows.append((name, c, *cells))
    md(["predicate", "column", "shuffle: prunable", "keyorder: prunable"], rows)


# ------------------------------------------------------------------ microbenchmarks
Q_SQL = {
    "A filter on dict string: count(*) where l_shipmode in (MAIL,SHIP)": "select count(*) from L where l_shipmode in ('MAIL','SHIP')",
    "B group by dict string: l_shipmode, count(*)": "select l_shipmode,count(*) from L group by 1",
    "C filter on dict int32: count(*) where l_shipdate in 1994": "select count(*) from L where l_shipdate>=date '1994-01-01' and l_shipdate<date '1995-01-01'",
    "D payload after dict filter: sum(l_extendedprice) where l_shipmode in (MAIL,SHIP)": "select sum(l_extendedprice) from L where l_shipmode in ('MAIL','SHIP')",
    "D0 payload, no filter: sum(l_extendedprice)": "select sum(l_extendedprice) from L",
    "D1 payload after 1.25%-selective filter: sum(l_extendedprice) where l_shipdate in 1995-09": "select sum(l_extendedprice) from L where l_shipdate>=date '1995-09-01' and l_shipdate<date '1995-10-01'",
    "E high-cardinality int: max(l_orderkey)": "select max(l_orderkey) from L",
    "F materialize dict string: max(length(l_shipinstruct))": "select max(length(l_shipinstruct)) from L",
    "G q6 (3 dict filters + 2 payload columns)": "select sum(l_extendedprice*l_discount) from L where l_shipdate>=date '1994-01-01' and l_shipdate<date '1995-01-01' and l_discount between 0.05 and 0.07 and l_quantity<24",
}


def _report(title, out):
    print(f"\n| {title} | dict | plain | delta | plain/dict | delta/dict |\n|---|---|---|---|---|---|")
    for name in Q_SQL:
        d, p, e = [out[(v, name)] for v in V3]
        print(f"| {name} | {d:.2f} | {p:.2f} | {e:.2f} | {p / d:.2f}x | {e / d:.2f}x |")


def micro_duckdb():
    import duckdb
    out = {}
    for v in V3:
        con = duckdb.connect(); con.execute("set threads=64")
        con.execute(f"create view L as select * from read_parquet('{R}/{v}/parquet/lineitem/*.parquet')")
        for name, q in Q_SQL.items():
            ts = []
            for _ in range(3):
                t = time.perf_counter(); con.execute(q).fetchall(); ts.append(time.perf_counter() - t)
            out[(v, name)] = min(ts); print(f"{v:22s} {name[:60]:60s} {min(ts):.2f}s", file=sys.stderr, flush=True)
    _report("DuckDB 1.5.5, 64 threads, SF100 lineitem, page-cached NVMe, best of 3 (s)", out)


def micro_gpu():
    import polars as pl
    eng = pl.GPUEngine(device=0)
    Q = {
        list(Q_SQL)[0]: lambda lf: lf.filter(pl.col("l_shipmode").is_in(["MAIL", "SHIP"])).select(pl.len()),
        list(Q_SQL)[1]: lambda lf: lf.group_by("l_shipmode").agg(pl.len()),
        list(Q_SQL)[2]: lambda lf: lf.filter((pl.col("l_shipdate") >= pl.date(1994, 1, 1)) & (pl.col("l_shipdate") < pl.date(1995, 1, 1))).select(pl.len()),
        list(Q_SQL)[3]: lambda lf: lf.filter(pl.col("l_shipmode").is_in(["MAIL", "SHIP"])).select(pl.col("l_extendedprice").sum()),
        list(Q_SQL)[4]: lambda lf: lf.select(pl.col("l_extendedprice").sum()),
        list(Q_SQL)[5]: lambda lf: lf.filter((pl.col("l_shipdate") >= pl.date(1995, 9, 1)) & (pl.col("l_shipdate") < pl.date(1995, 10, 1))).select(pl.col("l_extendedprice").sum()),
        list(Q_SQL)[6]: lambda lf: lf.select(pl.col("l_orderkey").max()),
        list(Q_SQL)[7]: lambda lf: lf.select(pl.col("l_shipinstruct").str.len_bytes().max()),
        list(Q_SQL)[8]: lambda lf: lf.filter((pl.col("l_shipdate") >= pl.date(1994, 1, 1)) & (pl.col("l_shipdate") < pl.date(1995, 1, 1)) & (pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07) & (pl.col("l_quantity") < 24)).select((pl.col("l_extendedprice") * pl.col("l_discount")).sum()),
    }
    out = {}
    for v in V3:
        for name, f in Q.items():
            ts = []
            for _ in range(3):
                t = time.perf_counter(); f(pl.scan_parquet(f"{R}/{v}/parquet/lineitem/*.parquet")).collect(engine=eng); ts.append(time.perf_counter() - t)
            out[(v, name)] = min(ts); print(f"{v:22s} {name[:60]:60s} {min(ts):.2f}s", file=sys.stderr, flush=True)
    _report("cudf-polars 26.6 (in-memory executor), 1 H100, SF100 lineitem, page-cached NVMe, best of 3 (s)", out)


# ------------------------------------------------------------------ tables from pagestats.csv
def tables():
    rows = list(csv.DictReader(open(os.path.join(HERE, "format_ablation_pagestats.csv"))))
    g = lambda r, k: float(r.get(k) or 0)
    Rw = {(r['variant'], r['table'], r['column']): r for r in rows}
    order = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment',
             'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment',
             'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment', 'p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment',
             'c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']
    T = {'l': 'lineitem', 'o': 'orders', 'ps': 'partsupp', 'p': 'part', 'c': 'customer'}
    print("### T1. Dictionary anatomy, shuffle-dict-snappy (16 sampled files per table; one row group per file)\n")
    out = []
    for c in order:
        r = Rw.get(('shuffle-dict-snappy', T[c.split('_')[0]], c))
        if not r: continue
        ch, dp = g(r, 'chunks'), g(r, 'dict_pages')
        if dp == 0:
            out.append((c, r['ptype'], "PLAIN fallback", "", "", "", "", "", f"{g(r, 'data_pages') / ch:.0f}")); continue
        out.append((c, r['ptype'], f"{int(dp)}/{int(ch)}", f"{g(r, 'dict_entries') / dp:,.0f}", f"{g(r, 'dict_comp_bytes') / dp:,.0f}",
                    f"{100 * g(r, 'dict_comp_bytes') / g(r, 'comp_bytes'):.2f}%", f"{g(r, 'idx_bits_sum') / max(1, g(r, 'idx_vals')):.0f}",
                    f"{100 * g(r, 'rle_vals') / max(1, g(r, 'rle_vals') + g(r, 'bp_vals')):.1f}%", f"{g(r, 'data_pages') / ch:.0f}"))
    md(["column", "type", "dict pages/chunks", "entries per chunk", "dict bytes per chunk", "dict share of chunk bytes", "index bits/value", "values in RLE runs", "data pages/chunk"], out)
    print("### T2. keyorder-dict-snappy: columns whose dictionary anatomy changes\n")
    out = []
    for c in order:
        r, s = Rw.get(('keyorder-dict-snappy', T[c.split('_')[0]], c)), Rw.get(('shuffle-dict-snappy', T[c.split('_')[0]], c))
        if not r or not s: continue
        dp = g(r, 'dict_pages'); rle = g(r, 'rle_vals') / max(1, g(r, 'rle_vals') + g(r, 'bp_vals')); srle = g(s, 'rle_vals') / max(1, g(s, 'rle_vals') + g(s, 'bp_vals'))
        ratio = g(r, 'comp_bytes') / g(s, 'comp_bytes')
        if abs(ratio - 1) < 0.05 and abs(rle - srle) < 0.02 and dp == g(s, 'dict_pages'): continue
        out.append((c, f"{int(dp)}/{int(g(r, 'chunks'))}", f"{g(r, 'dict_entries') / dp:,.0f}" if dp else "PLAIN fallback", f"{g(r, 'idx_bits_sum') / max(1, g(r, 'idx_vals')):.1f}" if dp else "", f"{100 * rle:.1f}%", f"{ratio:.2f}x"))
    md(["column", "dict pages/chunks", "entries per chunk", "index bits/value", "values in RLE runs", "chunk bytes vs shuffle"], out)
    print("### T3. DELTA_BINARY_PACKED: miniblock (32 values) bit widths, shuffle vs keyorder\n")
    out = []
    for c in order:
        for v in ['shuffle-delta-snappy', 'keyorder-delta-snappy']:
            r = Rw.get((v, T[c.split('_')[0]], c))
            if not r or g(r, 'mb') == 0: continue
            mb = g(r, 'mb'); ws = {int(k[1:]): g(r, k) for k in r if k.startswith('w') and k[1:].isdigit()}
            top = sorted(ws.items(), key=lambda x: -x[1])[:3]
            out.append((c, r['ptype'], v.split('-')[0], f"{g(r, 'mb_bits') / mb:.1f}", f"{100 * ws.get(0, 0) / mb:.0f}%", " ".join(f"{w}b:{100 * n / mb:.0f}%" for w, n in top)))
    md(["column", "type", "layout", "mean bits/value", "width-0 miniblocks", "top widths"], out)
    print("### T4. DELTA_BYTE_ARRAY: shared prefix vs stored suffix (shuffle-delta-snappy)\n")
    out = []
    for c in order:
        r = Rw.get(('shuffle-delta-snappy', T[c.split('_')[0]], c))
        if not r or g(r, 'dba_vals') == 0: continue
        n, ps, ss = g(r, 'dba_vals'), g(r, 'prefix_sum'), g(r, 'suffix_sum')
        out.append((c, f"{ps / n:.1f}", f"{ss / n:.1f}", f"{100 * ps / (ps + ss):.0f}%"))
    md(["column", "mean prefix chars (shared)", "mean suffix chars (stored)", "prefix share"], out)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "tables"
    {"dict-hits": dict_hits, "zonemap": zonemap, "micro-duckdb": micro_duckdb, "micro-gpu": micro_gpu, "tables": tables}[cmd]()
