# Collect per-column compressed/uncompressed bytes + encodings from parquet footers of every
# format variant: python3 format_ablation_colsizes.py <out.csv> [variants_root]   (needs pyarrow)
import os, sys, glob, csv
from concurrent.futures import ProcessPoolExecutor
import pyarrow.parquet as pq
ROOT=sys.argv[2] if len(sys.argv)>2 else "/data/haotiang/parquet-ablation/fmt_sf100"
VARS=[v for v in sorted(os.listdir(ROOT)) if not v.startswith("_")]
TABLES=["lineitem","orders","partsupp","part","customer","supplier","nation","region"]
def one_file(path):
    md=pq.ParquetFile(path).metadata
    out={}
    for rg in range(md.num_row_groups):
        r=md.row_group(rg)
        for c in range(r.num_columns):
            cc=r.column(c)
            k=cc.path_in_schema
            d=out.setdefault(k,[0,0,set(),cc.physical_type,0])
            d[0]+=cc.total_compressed_size; d[1]+=cc.total_uncompressed_size
            d[2].update(cc.encodings); d[4]+=cc.num_values
    return out
def one_table(args):
    v,t=args
    files=glob.glob(f"{ROOT}/{v}/parquet/{t}/*.parquet")
    agg={}
    for f in files:
        for k,(cb,ub,enc,typ,nv) in one_file(f).items():
            d=agg.setdefault(k,[0,0,set(),typ,0])
            d[0]+=cb; d[1]+=ub; d[2]|=enc; d[4]+=nv
    return [(v,t,k,typ,cb,ub,nv,"|".join(sorted(e))) for k,(cb,ub,e,typ,nv) in agg.items()]
if __name__=="__main__":
    jobs=[(v,t) for v in VARS for t in TABLES]
    w=csv.writer(open(sys.argv[1],"w"))
    w.writerow(["variant","table","column","ptype","compressed","uncompressed","num_values","encodings"])
    with ProcessPoolExecutor(32) as ex:
        for rows in ex.map(one_table,jobs):
            w.writerows(rows); print(rows[0][:2],len(rows),flush=True)
