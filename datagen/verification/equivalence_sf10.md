# Dataset equivalence: upstream vs datagen
- upstream: /dev/shm/verify/upstream_sf10/parquet
- datagen: /dev/shm/verify/ours_sf10/parquet

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
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 100/200 vs 100/200
  [OK  ] total rows  100,000 vs 100,000
  [info] compressed bytes 9,027,508 vs 9,027,508 (uncompressed 15,592,025 vs 15,592,025) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, s_acctbal:BIT_PACKED/PLAIN/RLE, s_address:BIT_PACKED/PLAIN/RLE, s_comment:BIT_PACKED/PLAIN/RLE, s_name:BIT_PACKED/PLAIN/RLE, s_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, s_phone:BIT_PACKED/PLAIN/RLE, s_suppkey:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## customer
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 1496/3005 vs 1496/3005
  [OK  ] total rows  1,500,000 vs 1,500,000
  [info] compressed bytes 127,909,474 vs 127,909,474 (uncompressed 245,602,714 vs 245,602,714) -- not compared; row order affects compression
  [info] encodings: c_acctbal:BIT_PACKED/PLAIN/RLE, c_address:BIT_PACKED/PLAIN/RLE, c_comment:BIT_PACKED/PLAIN/RLE, c_custkey:BIT_PACKED/PLAIN/RLE, c_mktsegment:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_name:BIT_PACKED/PLAIN/RLE, c_nationkey:BIT_PACKED/PLAIN_DICTIONARY/RLE, c_phone:BIT_PACKED/PLAIN/RLE, ignore:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## part
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 1998/4002 vs 1998/4002
  [OK  ] total rows  2,000,000 vs 2,000,000
  [info] compressed bytes 70,624,635 vs 70,624,635 (uncompressed 150,645,037 vs 150,645,037) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, p_brand:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_comment:BIT_PACKED/PLAIN/RLE, p_container:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_mfgr:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_name:BIT_PACKED/PLAIN/RLE, p_partkey:BIT_PACKED/PLAIN/RLE, p_retailprice:BIT_PACKED/PLAIN/RLE, p_size:BIT_PACKED/PLAIN_DICTIONARY/RLE, p_type:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## partsupp
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 7993/16008 vs 7993/16008
  [OK  ] total rows  8,000,000 vs 8,000,000
  [info] compressed bytes 475,848,473 vs 475,848,473 (uncompressed 1,241,687,119 vs 1,241,687,119) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, ps_availqty:BIT_PACKED/PLAIN/PLAIN_DICTIONARY/RLE, ps_comment:BIT_PACKED/PLAIN/RLE, ps_partkey:BIT_PACKED/PLAIN/RLE, ps_suppkey:BIT_PACKED/PLAIN/RLE, ps_supplycost:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## orders
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 14994/30009 vs 14994/30009
  [OK  ] total rows  15,000,000 vs 15,000,000
  [info] compressed bytes 621,967,010 vs 621,967,010 (uncompressed 1,313,249,027 vs 1,313,249,027) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, o_clerk:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_comment:BIT_PACKED/PLAIN/RLE, o_custkey:BIT_PACKED/PLAIN/RLE, o_orderdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderkey:BIT_PACKED/PLAIN/RLE, o_orderpriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_orderstatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_shippriority:BIT_PACKED/PLAIN_DICTIONARY/RLE, o_totalprice:BIT_PACKED/PLAIN/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## lineitem
  [OK  ] arrow schema (types, nullability, spark metadata)
  [OK  ] parquet schema (physical/logical types, repetition)
  [OK  ] writer  ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)'] vs ['parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)']
  [OK  ] compression codecs per column  ['SNAPPY']
  [OK  ] encodings per column
  [OK  ] file count  600 vs 600
  [OK  ] row groups  600 vs 600
  [OK  ] rows per file (multiset)  min/max 59976/119990 vs 59976/119990
  [OK  ] total rows  59,986,052 vs 59,986,052
  [info] compressed bytes 2,412,531,383 vs 2,412,531,383 (uncompressed 4,228,515,792 vs 4,228,515,792) -- not compared; row order affects compression
  [info] encodings: ignore:BIT_PACKED/PLAIN/RLE, l_comment:BIT_PACKED/PLAIN/RLE, l_commitdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_discount:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_extendedprice:BIT_PACKED/PLAIN/RLE, l_linenumber:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_linestatus:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_orderkey:BIT_PACKED/PLAIN/RLE, l_partkey:BIT_PACKED/PLAIN/RLE, l_quantity:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_receiptdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_returnflag:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipdate:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipinstruct:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_shipmode:BIT_PACKED/PLAIN_DICTIONARY/RLE, l_suppkey:BIT_PACKED/PLAIN/RLE, l_tax:BIT_PACKED/PLAIN_DICTIONARY/RLE
  [OK  ] rows A EXCEPT ALL B = 0 and B EXCEPT ALL A = 0  (0, 0)

## Verdict
EQUIVALENT: upstream and datagen hold identical rows in an identical parquet schema and physical layout (writer, codec, encodings, file/row-group structure).
