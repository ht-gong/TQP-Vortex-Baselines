# Dataset equivalence: upstream vs datagen
- upstream: /data/haotiang/parquet-ablation/fmt_sf100/_upstream/parquet
- datagen: /data/haotiang/parquet-ablation/fmt_sf100/shuffle-dict-snappy/parquet

## region
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  6 vs 6
  [OK  ] row groups  5 vs 5
  [OK  ] rows per file (multiset)  min/max 0/1 vs 0/1
  [OK  ] total rows  5 vs 5
  [info] compressed bytes 1,058 vs 1,058 (uncompressed 1,028 vs 1,028) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, r_comment:BIT_PACKED/PLAIN/RLE, r_name:BIT_PACKED/PLAIN/RLE, r_regionkey:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## nation
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  26 vs 26
  [OK  ] row groups  25 vs 25
  [OK  ] rows per file (multiset)  min/max 0/1 vs 0/1
  [OK  ] total rows  25 vs 25
  [info] compressed bytes 6,533 vs 6,533 (uncompressed 6,287 vs 6,287) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, n_comment:BIT_PACKED/PLAIN/RLE, n_name:BIT_PACKED/PLAIN/RLE, n_nationkey:BIT_PACKED/PLAIN/RLE, n_regionkey:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## supplier
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 625/625 vs 625/625
  [OK  ] total rows  1,000,000 vs 1,000,000
  [info] compressed bytes 84,171,730 vs 84,171,730 (uncompressed 153,911,396 vs 153,911,396) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, s_acctbal:BIT_PACKED/PLAIN/RLE, s_address:BIT_PACKED/PLAIN/RLE, s_comment:BIT_PACKED/PLAIN/RLE, s_name:BIT_PACKED/PLAIN/RLE, s_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, s_phone:BIT_PACKED/PLAIN/RLE, s_suppkey:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## customer
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 9365/9385 vs 9365/9385
  [OK  ] total rows  15,000,000 vs 15,000,000
  [info] compressed bytes 1,272,399,644 vs 1,272,399,644 (uncompressed 2,453,347,538 vs 2,453,347,538) -- not compared; row order affects compression
  [info] encodings: c_acctbal:BIT_PACKED/PLAIN/RLE, c_address:BIT_PACKED/PLAIN/RLE, c_comment:BIT_PACKED/PLAIN/RLE, c_custkey:BIT_PACKED/PLAIN/RLE, c_mktsegment:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_name:BIT_PACKED/PLAIN/RLE, c_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_phone:BIT_PACKED/PLAIN/RLE, ignore:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## part
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 12493/12508 vs 12493/12508
  [OK  ] total rows  20,000,000 vs 20,000,000
  [info] compressed bytes 691,574,623 vs 691,574,623 (uncompressed 1,483,522,068 vs 1,483,522,068) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, p_brand:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_comment:BIT_PACKED/PLAIN/RLE, p_container:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_mfgr:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_name:BIT_PACKED/PLAIN/RLE, p_partkey:BIT_PACKED/PLAIN/RLE, p_retailprice:BIT_PACKED/PLAIN/RLE, p_size:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_type:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## partsupp
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 49979/50019 vs 49979/50019
  [OK  ] total rows  80,000,000 vs 80,000,000
  [info] compressed bytes 4,670,032,559 vs 4,670,032,559 (uncompressed 12,325,081,198 vs 12,325,081,198) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, ps_availqty:BIT_PACKED/PLAIN_DICTIONARY/RLE, ps_comment:BIT_PACKED/PLAIN/RLE, ps_partkey:BIT_PACKED/PLAIN/RLE, ps_suppkey:BIT_PACKED/PLAIN/RLE, ps_supplycost:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## orders
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 93733/93766 vs 93733/93766
  [OK  ] total rows  150,000,000 vs 150,000,000
  [info] compressed bytes 6,645,276,643 vs 6,645,276,643 (uncompressed 14,662,104,117 vs 14,662,104,117) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, o_clerk:BIT_PACKED/PLAIN/PLAIN_DICTIONARY/RLE, o_comment:BIT_PACKED/PLAIN/RLE, o_custkey:BIT_PACKED/PLAIN/RLE, o_orderdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderkey:BIT_PACKED/PLAIN/RLE, o_orderpriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderstatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_shippriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_totalprice:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## lineitem
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  1600 vs 1600
  [OK  ] row groups  1600 vs 1600
  [OK  ] rows per file (multiset)  min/max 374919/375081 vs 374919/375081
  [OK  ] total rows  600,037,902 vs 600,037,902
  [info] compressed bytes 24,600,594,367 vs 24,600,594,367 (uncompressed 42,160,500,708 vs 42,160,500,708) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, l_comment:BIT_PACKED/PLAIN/RLE, l_commitdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_discount:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_extendedprice:BIT_PACKED/PLAIN/RLE, l_linenumber:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_linestatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_orderkey:BIT_PACKED/PLAIN/RLE, l_partkey:BIT_PACKED/PLAIN/RLE, l_quantity:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_receiptdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_returnflag:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipinstruct:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipmode:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_suppkey:BIT_PACKED/PLAIN/RLE, l_tax:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## Verdict
EQUIVALENT: upstream and datagen hold identical rows in an identical parquet schema and physical layout (writer, codec, encodings, file/row-group structure).
