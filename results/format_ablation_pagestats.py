"""Page-level anatomy of the parquet-format ablation variants, read straight from the files
(no query engine): thrift page headers + the encoded page bodies.

  datagen-venv/bin/python results/format_ablation_pagestats.py <out.csv> [fmt_root] [variants] [tables] [files_per_table]

Defaults: fmt_root=/data/haotiang/parquet-ablation/fmt_sf100, variants = shuffle/keyorder x dict/delta (+shuffle-plain),
tables = lineitem,orders,partsupp,part,customer, 16 files per table (every 100th of the 1600).

Per (variant, table, column) it reports: chunks/pages, dictionary pages and ENTRIES (from the DictionaryPageHeader,
no decoding), dictionary bytes, RLE_DICTIONARY index bit width and the share of index values stored as RLE runs
vs bit-packed groups, DELTA_BINARY_PACKED miniblock bit-width histogram (w<k> columns), DELTA_BYTE_ARRAY
prefix/suffix lengths (fully decoded) and their length-stream bit widths (pw<k>/sw<k>), page-index presence.
Needs pyarrow (for snappy/zstd/lz4 page decompression) only. results/FORMAT_DECODING.md uses the output.
"""
import sys, os, glob, csv, struct, math
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
import pyarrow as pa, pyarrow.parquet as pq

# ---------- thrift compact ----------
def uvarint(b,p):
    r=0;s=0
    while True:
        x=b[p];p+=1;r|=(x&0x7f)<<s;s+=7
        if not x&0x80: return r,p
def zz(v): return (v>>1)^-(v&1)
def rd_val(b,p,t):
    if t in(1,2): return t==1,p
    if t==3: return struct.unpack_from('b',b,p)[0],p+1
    if t in(4,5,6): v,p=uvarint(b,p); return zz(v),p
    if t==7: return struct.unpack_from('<d',b,p)[0],p+8
    if t==8: n,p=uvarint(b,p); return bytes(b[p:p+n]),p+n
    if t in(9,10):
        h=b[p];p+=1;n=h>>4;et=h&15
        if n==15: n,p=uvarint(b,p)
        out=[]
        for _ in range(n): v,p=rd_val(b,p,et);out.append(v)
        return out,p
    if t==11:
        n,p=uvarint(b,p)
        if n==0: return {},p
        kt,vt=b[p]>>4,b[p]&15;p+=1;out={}
        for _ in range(n): k,p=rd_val(b,p,kt);v,p=rd_val(b,p,vt);out[k]=v
        return out,p
    if t==12: return rd_struct(b,p)
    raise ValueError(t)
def rd_struct(b,p):
    fid=0;out={}
    while True:
        h=b[p];p+=1
        if h==0: return out,p
        d=h>>4;t=h&15
        if d: fid+=d
        else:
            v,p=uvarint(b,p);fid=zz(v)
        if t in(1,2): out[fid]=(t==1);continue
        out[fid],p=rd_val(b,p,t)

# ---------- decoders ----------
def unpack_bits(data,n,w):
    """LSB-first bit-pack of n values of width w -> list of ints (pure python)"""
    if w==0: return [0]*n
    big=int.from_bytes(bytes(data[:(n*w+7)//8]),'little'); mask=(1<<w)-1
    return [(big>>(i*w))&mask for i in range(n)]
def delta_binary_packed(b,p,want_values=False):
    """returns (stats dict, values or None, p_end)"""
    bs,p=uvarint(b,p); nmb,p=uvarint(b,p); total,p=uvarint(b,p); fv,p=uvarint(b,p); fv=zz(fv)
    vpm=bs//nmb; widths=[]; remaining=total-1; vals=[fv] if want_values else None
    cur=fv
    while remaining>0:
        md,p=uvarint(b,p); md=zz(md)
        ws=list(b[p:p+nmb]); p+=nmb
        for w in ws:
            if remaining<=0: break
            k=min(vpm,remaining)
            widths.append(w)
            if want_values:
                for x in unpack_bits(b[p:p+vpm*w//8],vpm,w)[:k]:
                    cur+=x+md; vals.append(cur)
            p+=vpm*w//8
            remaining-=k
    return dict(total=total,nmb=nmb,bs=bs,widths=widths),vals,p

def decompress(codec,data,usize):
    if codec=='UNCOMPRESSED': return data
    c={'SNAPPY':'snappy','ZSTD':'zstd','LZ4_RAW':'lz4_raw','GZIP':'gzip'}[codec]
    return pa.decompress(data,decompressed_size=usize,codec=c)

def rle_scan(b,p,end,w):
    """RLE/bit-packed hybrid: count values in RLE runs vs bit-packed groups"""
    rle_vals=0;bp_vals=0;runs=0;groups=0
    while p<end:
        h,p=uvarint(b,p)
        if h&1:
            g=h>>1; p+=g*w; bp_vals+=g*8; groups+=1
        else:
            n=h>>1; p+=(w+7)//8; rle_vals+=n; runs+=1
    return rle_vals,bp_vals,runs,groups

ENC={0:'PLAIN',2:'PLAIN_DICTIONARY',3:'RLE',4:'BIT_PACKED',5:'DELTA_BINARY_PACKED',6:'DELTA_LENGTH_BYTE_ARRAY',7:'DELTA_BYTE_ARRAY',8:'RLE_DICTIONARY',9:'BYTE_STREAM_SPLIT'}

def one_file(path):
    pf=pq.ParquetFile(path); md=pf.metadata
    f=open(path,'rb'); raw=f.read(); f.close(); b=memoryview(raw)
    res={}
    for rg in range(md.num_row_groups):
        r=md.row_group(rg)
        for ci in range(r.num_columns):
            cc=r.column(ci); col=cc.path_in_schema
            if col=='ignore': continue
            st=res.setdefault(col,defaultdict(float))
            st['ptype']=cc.physical_type; st['codec']=cc.compression
            st['has_colidx']=int(bool(cc.has_column_index)); st['has_offidx']=int(bool(cc.has_offset_index))
            st['chunks']+=1; st['values']+=cc.num_values; st['comp_bytes']+=cc.total_compressed_size
            start=min(x for x in (cc.dictionary_page_offset,cc.data_page_offset) if x is not None)
            p=start; end=start+cc.total_compressed_size
            page_enc=Counter()
            while p<end:
                hdr,p2=rd_struct(b,p); ptype=hdr[1]; usz=hdr[2]; csz=hdr[3]
                body=b[p2:p2+csz]; p=p2+csz
                if ptype==2:  # dictionary page
                    dh=hdr[7]; st['dict_pages']+=1; st['dict_entries']+=dh[1]; st['dict_comp_bytes']+=csz; st['dict_uncomp_bytes']+=usz
                    continue
                if ptype==0:
                    dh=hdr[5]; enc=ENC.get(dh[2],dh[2]); nv=dh[1]; page_enc[enc]+=1
                    st['data_pages']+=1; st['data_comp_bytes']+=csz; st['page_has_stats']+= int(5 in dh)
                    if enc in('RLE_DICTIONARY','PLAIN_DICTIONARY'):
                        d=decompress(cc.compression,body,usz); q=0
                        # def levels (optional column): 4-byte length + RLE
                        dl=struct.unpack_from('<I',d,0)[0]; q=4+dl
                        w=d[q]; q+=1
                        st['idx_pages']+=1; st['idx_bits_sum']+=w*nv; st['idx_vals']+=nv
                        rv,bv,runs,groups=rle_scan(d,q,len(d),w)
                        st['rle_vals']+=rv; st['bp_vals']+=bv; st['rle_runs']+=runs; st['bp_groups']+=groups
                    elif enc=='PLAIN':
                        st['plain_pages']+=1; st['plain_vals']+=nv
                elif ptype==3:
                    dh=hdr[8]; enc=ENC.get(dh[4],dh[4]); nv=dh[1]; page_enc[enc]+=1
                    st['data_pages']+=1; st['data_comp_bytes']+=csz; st['page_has_stats']+= int(8 in dh)
                    dlen=dh.get(5,0)+dh.get(6,0)
                    d=body[dlen:]
                    if dh.get(7,True): d=decompress(cc.compression,d,usz-dlen)
                    d=bytes(d)
                    if enc=='DELTA_BINARY_PACKED':
                        s,_,_=delta_binary_packed(d,0)
                        st['dbp_pages']+=1; st['dbp_vals']+=s['total']
                        for w in s['widths']:
                            st[f'w{w}']+=1; st['mb']+=1; st['mb_bits']+=w
                    elif enc=='DELTA_BYTE_ARRAY':
                        s1,pref,q=delta_binary_packed(d,0,True)
                        s2,suf,q2=delta_binary_packed(d,q,True)
                        st['dba_pages']+=1; st['dba_vals']+=s1['total']
                        st['prefix_sum']+=sum(pref); st['suffix_sum']+=sum(suf)
                        for w in s1['widths']: st[f'pw{w}']+=1; st['pmb']+=1
                        for w in s2['widths']: st[f'sw{w}']+=1; st['smb']+=1
            for e,n in page_enc.items(): st['enc_'+e]+=n
    return res

def run(args):
    v,t,files=args
    agg={}
    for f in files:
        for col,st in one_file(f).items():
            a=agg.setdefault(col,defaultdict(float))
            for k,x in st.items():
                if isinstance(x,str): a[k]=x
                else: a[k]+=x
    return v,t,agg

if __name__=='__main__':
    a=sys.argv[1:]
    out=a[0]
    root=a[1] if len(a)>1 else "/data/haotiang/parquet-ablation/fmt_sf100"
    variants=a[2].split(',') if len(a)>2 else ["shuffle-dict-snappy","keyorder-dict-snappy","shuffle-delta-snappy","keyorder-delta-snappy","shuffle-plain-snappy"]
    tables=a[3].split(',') if len(a)>3 else ["lineitem","orders","partsupp","part","customer"]
    nf=int(a[4]) if len(a)>4 else 16
    jobs=[]
    for v in variants:
        for t in tables:
            fs=sorted(glob.glob(f"{root}/{v}/parquet/{t}/*.parquet"))
            step=max(1,len(fs)//nf); fs=fs[::step][:nf]
            jobs.append((v,t,fs))
    rows=[]
    with ProcessPoolExecutor(min(48,len(jobs))) as ex:
        for v,t,agg in ex.map(run,jobs):
            for col,st in agg.items():
                rows.append(dict(variant=v,table=t,column=col,**{k:(x if isinstance(x,str) else round(x,3)) for k,x in st.items()}))
            print(v,t,len(agg),flush=True)
    keys=sorted({k for r in rows for k in r},key=lambda k:(k not in('variant','table','column'),k))
    w=csv.DictWriter(open(out,'w'),fieldnames=keys); w.writeheader(); w.writerows(rows)
