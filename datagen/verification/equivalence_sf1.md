# Dataset equivalence: upstream vs datagen
- upstream: /dev/shm/verify/upstream_sf1/parquet
- datagen: /dev/shm/verify/ours_sf1/parquet
- raw dbgen text: /dev/shm/verify/raw_sf1

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
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

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
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## supplier
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 24/26 vs 24/26
  [OK  ] total rows  10,000 vs 10,000
  [info] compressed bytes 1,174,283 vs 1,174,283 (uncompressed 1,690,103 vs 1,690,103) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, s_acctbal:BIT_PACKED/PLAIN/RLE, s_address:BIT_PACKED/PLAIN/RLE, s_comment:BIT_PACKED/PLAIN/RLE, s_name:BIT_PACKED/PLAIN/RLE, s_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, s_phone:BIT_PACKED/PLAIN/RLE, s_suppkey:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## customer
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 374/377 vs 374/377
  [OK  ] total rows  150,000 vs 150,000
  [info] compressed bytes 13,479,248 vs 13,479,248 (uncompressed 24,778,904 vs 24,778,904) -- not compared; row order affects compression
  [info] encodings: c_acctbal:BIT_PACKED/PLAIN/RLE, c_address:BIT_PACKED/PLAIN/RLE, c_comment:BIT_PACKED/PLAIN/RLE, c_custkey:BIT_PACKED/PLAIN/RLE, c_mktsegment:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_name:BIT_PACKED/PLAIN/RLE, c_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_phone:BIT_PACKED/PLAIN/RLE, ignore:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## part
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 498/501 vs 498/501
  [OK  ] total rows  200,000 vs 200,000
  [info] compressed bytes 8,139,006 vs 8,139,006 (uncompressed 16,780,377 vs 16,780,377) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, p_brand:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_comment:BIT_PACKED/PLAIN/RLE, p_container:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_mfgr:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_name:BIT_PACKED/PLAIN/RLE, p_partkey:BIT_PACKED/PLAIN/RLE, p_retailprice:BIT_PACKED/PLAIN/RLE, p_size:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_type:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## partsupp
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 1997/2003 vs 1997/2003
  [OK  ] total rows  800,000 vs 800,000
  [info] compressed bytes 47,487,646 vs 47,487,646 (uncompressed 124,573,064 vs 124,573,064) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, ps_availqty:BIT_PACKED/PLAIN/RLE, ps_comment:BIT_PACKED/PLAIN/RLE, ps_partkey:BIT_PACKED/PLAIN/RLE, ps_suppkey:BIT_PACKED/PLAIN/RLE, ps_supplycost:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## orders
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 3747/3753 vs 3747/3753
  [OK  ] total rows  1,500,000 vs 1,500,000
  [info] compressed bytes 62,511,127 vs 62,511,127 (uncompressed 130,332,235 vs 130,332,235) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, o_clerk:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_comment:BIT_PACKED/PLAIN/RLE, o_custkey:BIT_PACKED/PLAIN/RLE, o_orderdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderkey:BIT_PACKED/PLAIN/RLE, o_orderpriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderstatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_shippriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_totalprice:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## lineitem
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  400 vs 400
  [OK  ] row groups  400 vs 400
  [OK  ] rows per file (multiset)  min/max 14989/15019 vs 14989/15019
  [OK  ] total rows  6,001,215 vs 6,001,215
  [info] compressed bytes 244,049,715 vs 244,049,715 (uncompressed 420,069,697 vs 420,069,697) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, l_comment:BIT_PACKED/PLAIN/RLE, l_commitdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_discount:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_extendedprice:BIT_PACKED/PLAIN/RLE, l_linenumber:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_linestatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_orderkey:BIT_PACKED/PLAIN/RLE, l_partkey:BIT_PACKED/PLAIN/RLE, l_quantity:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_receiptdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_returnflag:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipinstruct:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipmode:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_suppkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_tax:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)
  [OK  ] raw dbgen text == upstream parquet  (0, 0)
  [OK  ] raw dbgen text == datagen parquet  (0, 0)
  [OK  ] `ignore` column all NULL in both  non-null: 0, 0

## Verdict
EQUIVALENT: upstream and datagen hold identical rows in an identical parquet schema and physical layout (writer, codec, encodings, file/row-group structure).
