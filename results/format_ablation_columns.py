"""Per-column size analysis of the SF100 parquet-format ablation.
Reads results/format_ablation_colsizes.csv (parquet-footer bytes per variant/table/column,
from format_ablation_colsizes.py) and results/format_ablation.csv (timings), prints markdown:
  A. per-column compressed size by encoding, sorted by bytes moved
  B. best encoding per column, totals, keyorder effect
  C. codec ratio per encoding / column class
  D. size vs execution time (variant level, per-query touched bytes, per-factor)
Usage: datagen-venv/bin/python results/format_ablation_columns.py > results/format_ablation_columns.md
"""
import re, csv, math, statistics as st
from collections import defaultdict
import duckdb
import os
REPO=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
con=duckdb.connect()
con.execute(f"create table cs as select variant, \"table\" tbl, \"column\" col, ptype, compressed, uncompressed, num_values, encodings from read_csv('{REPO}/results/format_ablation_colsizes.csv') where \"column\"<>'ignore'")
con.execute("alter table cs add column layout varchar; alter table cs add column enc varchar; alter table cs add column codec varchar")
con.execute("update cs set layout=split_part(variant,'-',1), enc=split_part(variant,'-',2), codec=split_part(variant,'-',3)")
MB=1/2**20; GB=1/2**30
def q(sql): return con.execute(sql).fetchall()
def tbl(hdr, rows):
    print("| "+" | ".join(hdr)+" |"); print("|"+"---|"*len(hdr))
    for r in rows: print("| "+" | ".join(str(x) for x in r)+" |")
    print()
def f(x,d=1): return f"{x:.{d}f}"

# ---------- A. encoding movers (shuffle, snappy) ----------
print("## A. Per-column compressed size by encoding (shuffle layout, snappy), sorted by bytes moved plain->best\n")
rows=q("""
with p as (select tbl as table_name, col, ptype,
  max(case when enc='plain' then compressed end) pl,
  max(case when enc='dict' then compressed end) di,
  max(case when enc='delta' then compressed end) de,
  max(case when enc='dict' then encodings end) dict_enc,
  max(case when enc='plain' then uncompressed end) raw
 from cs where layout='shuffle' and codec='snappy' group by 1,2,3)
select table_name,col,ptype,pl,di,de,dict_enc,raw from p order by (pl - least(pl,di,de)) desc""")
out=[]
for t,c,pt,pl,di,de,denc,raw in rows:
    best=min(pl,di,de); bname={pl:"plain",di:"dict",de:"delta"}[best]
    fb="dict" if di<0.99*pl else "PLAIN-fallback"
    out.append((t,c,pt,f(pl*MB,0),f(di*MB,0),f(de*MB,0),bname,f(di/pl,2),f(de/di,2),f((pl-best)*MB,0),fb))
tbl(["table","column","type","plain MB","dict MB","delta MB","best","dict/plain","delta/dict","MB saved plain->best","dict variant used"],out)

# ---------- B. best encoding per column, trends ----------
print("## B. Best encoding per column (compressed, snappy, shuffle) -- counts by physical type and by dictionary applicability\n")
cnt=defaultdict(lambda: defaultdict(int)); bytes_=defaultdict(lambda: defaultdict(int))
for t,c,pt,pl,di,de,denc,raw in rows:
    best=min(pl,di,de); bname={pl:"plain",di:"dict",de:"delta"}[best]
    fb="dict-fallback-to-PLAIN" if di>=0.99*pl else "dict-encoded"
    cnt[(pt,fb)][bname]+=1
tbl(["type","dict variant","best=plain","best=dict","best=delta"],[(pt,fb,d["plain"],d["dict"],d["delta"]) for (pt,fb),d in sorted(cnt.items())])
print("Totals by encoding (shuffle, snappy), GiB, and sum of per-column best:")
tot=q("select enc, sum(compressed)/2^30 from cs where layout='shuffle' and codec='snappy' group by 1")
bestsum=sum(min(pl,di,de) for *_,pl,di,de,_,__ in [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in rows])*GB
tbl(["encoding","GiB"],[(e,f(g,2)) for e,g in tot]+[("per-column best (mixed)",f(bestsum,2))])
print("Keyorder vs shuffle (snappy) per encoding, GiB:")
tbl(["encoding","shuffle","keyorder","ratio"],[(e,f(a,2),f(b,2),f(b/a,2)) for e,a,b in q("""select enc, sum(case when layout='shuffle' then compressed end)/2^30, sum(case when layout='keyorder' then compressed end)/2^30 from cs where codec='snappy' group by 1 order by 1""")])
print("Keyorder biggest per-column changes vs shuffle (dict/snappy), MB:")
tbl(["table","column","enc","shuffle MB","keyorder MB","ratio"],[(t,c,e,f(a*MB,0),f(b*MB,0),f(b/a,2)) for t,c,e,a,b in q("""select tbl,col,enc, max(case when layout='shuffle' then compressed end) a, max(case when layout='keyorder' then compressed end) b from cs where codec='snappy' group by 1,2,3 order by abs(a-b) desc limit 12""")])

# ---------- C. compression ----------
print("## C. Codec effect per encoding (shuffle). 'encoded' = uncompressed page bytes after encoding; ratio = compressed/encoded\n")
tbl(["encoding","encoded GiB","snappy GiB","zstd GiB","lz4raw GiB","snappy ratio","zstd ratio","lz4 ratio"],
 [(e,f(u,2),f(s,2),f(z,2),f(l,2),f(s/u,2),f(z/u,2),f(l/u,2)) for e,u,s,z,l in q("""
 select enc, max(case when codec='snappy' then u end), max(case when codec='snappy' then c end), max(case when codec='zstd' then c end), max(case when codec='lz4raw' then c end)
 from (select enc,codec,sum(compressed)/2^30 c,sum(uncompressed)/2^30 u from cs where layout='shuffle' group by 1,2) group by 1 order by 1""")])
print("Per-column codec ratio (compressed/encoded), snappy vs zstd, by encoding -- columns where zstd gains most over snappy:")
tbl(["table","column","enc","encoded MB","snappy","zstd","lz4raw","zstd/snappy"],[(t,c,e,f(u*MB,0),f(s/u,2),f(z/u,2),f(l/u,2),f(z/s,2)) for t,c,e,u,s,z,l in q("""
 select tbl,col,enc, max(uncompressed) u, max(case when codec='snappy' then compressed end) s, max(case when codec='zstd' then compressed end) z, max(case when codec='lz4raw' then compressed end) l
 from cs where layout='shuffle' group by 1,2,3 order by (s-z) desc limit 15""")])
print("Codec ratio by (encoding, dictionary applicability) class, summed bytes:")
cls=q("""select enc, case when d.di<0.99*d.pl then 'dict-able' else 'high-card (PLAIN fallback)' end cls,
  sum(cs.uncompressed)/2^30 u, sum(case when codec='snappy' then compressed end)/2^30*3 s, sum(case when codec='zstd' then compressed end)/2^30*3 z, sum(case when codec='lz4raw' then compressed end)/2^30*3 l
 from cs join (select tbl,col,max(case when enc='dict' then compressed end) di,max(case when enc='plain' then compressed end) pl from cs where layout='shuffle' and codec='snappy' group by 1,2) d using(tbl,col) where layout='shuffle' group by 1,2 order by 2,1""")
tbl(["encoding","column class","encoded GiB","snappy ratio","zstd ratio","lz4 ratio"],[(e,c,f(u/3,2),f(s/u,2),f(z/u,2),f(l/u,2)) for e,c,u,s,z,l in cls])

# ---------- D. size vs speed ----------
print("## D. Size vs execution time\n")
sql=open(f"{REPO}/results/queries/stream_qualification.sql").read()
cols=set(r[0] for r in q("select distinct col from cs"))
parts=re.split(r"-- Template file: (\d+)",sql)
qcols={}
for i in range(1,len(parts),2):
    qn=int(parts[i]); body=parts[i+1]
    qcols[f"query{qn}"]=sorted(c for c in cols if re.search(r"\b"+c+r"\b",body))
for k in sorted(qcols,key=lambda s:int(s[5:])): print(f"{k}: {len(qcols[k])} cols"); 
print()
con.execute(f"create table r as select * from read_csv('{REPO}/results/format_ablation.csv') where scale_factor=100")
med=q("""select engine,variant,query,median(seconds) s, count(*) FILTER(status='OK') ok from r group by 1,2,3""")
sizes={(v,t,c):b for v,t,c,b in q("select variant,tbl,col,compressed from cs")}
def qbytes(v,qn): return sum(b for (vv,t,c),b in sizes.items() if vv==v and c in qcols[qn])
vars_=[r[0] for r in q("select distinct variant from cs")]
QB={(v,qn):qbytes(v,qn) for v in vars_ for qn in qcols}
def pearson(x,y):
    mx,my=st.mean(x),st.mean(y); sx=math.sqrt(sum((a-mx)**2 for a in x)); sy=math.sqrt(sum((b-my)**2 for b in y))
    return sum((a-mx)*(b-my) for a,b in zip(x,y))/(sx*sy) if sx and sy else float('nan')
def spearman(x,y):
    rk=lambda v:[sorted(v).index(a)+1 for a in v]; return pearson(rk(x),rk(y))
def slope(x,y):
    mx,my=st.mean(x),st.mean(y); return sum((a-mx)*(b-my) for a,b in zip(x,y))/sum((a-mx)**2 for a in x)
eng=["duckdb_cpu","sirius","polars_gpu","rapids"]
print("Variant-level: total dataset GiB vs sum of per-query medians (all 12 variants; rapids delta-zstd excluded due to timeouts):")
vsz={v:b*GB for v,b in q("select variant,sum(compressed) from cs group by 1")}
out=[]
for e in eng:
    tot=defaultdict(float); okc=defaultdict(int)
    for ee,v,qn,s,ok in med:
        if ee==e and ok==3: tot[v]+=s; okc[v]+=1
    vs=[v for v in vars_ if okc[v]==22]
    x=[vsz[v] for v in vs]; y=[tot[v] for v in vs]
    out.append((e,len(vs),f(pearson(x,y),2),f(spearman(x,y),2),f(slope(x,y),2)))
tbl(["engine","n variants","pearson","spearman","slope s/GiB"],out)
print("Query-level: bytes of columns the query touches vs median seconds. within-query = mean Pearson across the 12 variants per query (isolates format effect); pooled = over all (query,variant) points:")
out=[]
for e in eng:
    within=[]; px=[]; py=[]; slopes=[]
    for qn in qcols:
        pts=[(QB[(v,qn)]*GB,s) for ee,v,q2,s,ok in med if ee==e and q2==qn and ok==3]
        if len(pts)<6: continue
        x,y=zip(*pts); px+=x; py+=y
        within.append(pearson(x,y)); slopes.append(slope(x,y))
    out.append((e,len(within),f(st.mean(within),2),f(st.median(within),2),f"{sum(1 for w in within if w>0.5)}/{len(within)}",f(pearson(px,py),2),f(spearman(px,py),2),f(st.median(slopes),2)))
tbl(["engine","queries","mean within-q r","median within-q r","queries r>0.5","pooled pearson","pooled spearman","median slope s/GiB"],out)
print("Same, restricted to shuffle layout and separately per factor (codec-only: fix encoding, vary codec; encoding-only: fix codec=snappy, vary encoding):")
out=[]
for e in eng:
  for label,filt in [("codec only (3 codecs x 3 enc, within enc)",lambda v:v.startswith("shuffle")),("encoding only (snappy, shuffle)",lambda v:v.startswith("shuffle") and v.endswith("snappy"))]:
    within=[]
    for qn in qcols:
        if label.startswith("codec"):
            for en in ["plain","dict","delta"]:
                pts=[(QB[(v,qn)]*GB,s) for ee,v,q2,s,ok in med if ee==e and q2==qn and ok==3 and filt(v) and v.split('-')[1]==en]
                if len(pts)==3: within.append(pearson(*zip(*pts)))
        else:
            pts=[(QB[(v,qn)]*GB,s) for ee,v,q2,s,ok in med if ee==e and q2==qn and ok==3 and filt(v)]
            if len(pts)==3: within.append(pearson(*zip(*pts)))
    out.append((e,label,len(within),f(st.mean(within),2),f"{sum(1 for w in within if w>0.5)}/{len(within)}"))
tbl(["engine","factor","n (query x group)","mean r","r>0.5"],out)
print("Per-query: touched bytes (dict-snappy) and seconds; ratio delta/dict for bytes and for each engine's time (shuffle snappy):")
out=[]
for qn in sorted(qcols,key=lambda s:int(s[5:])):
    bd=QB[("shuffle-dict-snappy",qn)]; be=QB[("shuffle-delta-snappy",qn)]; bp=QB[("shuffle-plain-snappy",qn)]
    row=[qn,f(bd*GB,2),f(bp/bd,2),f(be/bd,2)]
    for e in eng:
        d={v:s for ee,v,q2,s,ok in med if ee==e and q2==qn and ok==3}
        row+= [f"{d.get('shuffle-dict-snappy',float('nan')):.2f}s p{d.get('shuffle-plain-snappy',float('nan'))/d['shuffle-dict-snappy']:.2f} d{d.get('shuffle-delta-snappy',float('nan'))/d['shuffle-dict-snappy']:.2f}" if 'shuffle-dict-snappy' in d else "-"]
    out.append(row)
tbl(["query","dict GiB touched","plain/dict bytes","delta/dict bytes"]+[e+" (dict s, plain x, delta x)" for e in eng],out)
