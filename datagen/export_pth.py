#!/usr/bin/env python3
"""Export a validated TPC-H parquet dataset as TQP-Vortex's `.pth` column files.

  export_pth.py <parquet_dir> <SF> <out_dir>          (make pth SF=<SF>)

This repo's generator is the ground truth for TQP-Vortex too: the export writes
the same files TQP-Vortex's own generator (tpch-dbgen-tensors, print.cpp) writes,
loadable by its unmodified loader, `list(torch.jit.load(path).parameters())[0]`:
one TorchScript archive per column, `<out_dir>/SF<SF>-tensor-<COL>.pth`, holding
the column as the module's single parameter "0" (what C++
`torch::save(tensor)` produces). The 54 columns are TQP-Vortex's; it skips
L_LINENUMBER, L_COMMENT, O_CLERK, P_COMMENT, PS_COMMENT, N_COMMENT, R_COMMENT.

Encoding (as print.cpp):
  keys (bigint) and the int columns   int64 [rows]
  l_quantity DECIMAL(11,2)            int64 [rows], the integral value
  other DECIMAL(11,2)                 float64 [rows], unscaled / 100
  dates                               int32 [rows], days since 1990-01-01
  O_ORDERSTATUS, L_RETURNFLAG,
  L_LINESTATUS                        int8 [rows], the character code
  other strings                       int8 [rows, width], bytes then NUL
                                      padding (strncpy: no terminator at full width)

Row order is the parquet order: a table's part-files in sorted name order, row
groups and rows as stored, so row i of every column of a table is the same TPC-H
row. Part-file names carry per-batch job UUIDs, so the order is fixed for a
dataset on disk but differs between two generations; <out_dir>/SF<SF>-manifest.json
records the part-file order and row counts.

Input guards fail the export: a null, a string longer than its width, a
fractional l_quantity, a date outside 1992-1998, a non-ASCII string. Files are
built in a temp dir and moved into <out_dir> only when every column succeeded.
"""
import datetime
import glob
import json
import os
import shutil
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import torch

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
SKIP = {"l_linenumber", "l_comment", "o_clerk", "p_comment", "ps_comment", "n_comment", "r_comment", "ignore"}
CHAR1 = {"o_orderstatus", "l_returnflag", "l_linestatus"}
WIDTH = {"c_name": 25, "c_address": 40, "c_phone": 15, "c_mktsegment": 10, "c_comment": 117,
         "o_orderpriority": 15, "o_comment": 79, "l_shipinstruct": 25, "l_shipmode": 10,
         "p_name": 55, "p_mfgr": 25, "p_brand": 10, "p_type": 25, "p_container": 10,
         "s_name": 25, "s_address": 40, "s_phone": 15, "s_comment": 101, "n_name": 25, "r_name": 25}
EPOCH = (datetime.date(1990, 1, 1) - datetime.date(1970, 1, 1)).days       # date32 -> TQP days
DATE_LO = (datetime.date(1992, 1, 1) - datetime.date(1970, 1, 1)).days
DATE_HI = (datetime.date(1998, 12, 31) - datetime.date(1970, 1, 1)).days


class Module(torch.nn.Module):
    """Names the archive's class __torch__.Module, as C++ torch::save does."""
    __module__ = "__main__"


def save(tensor, path):
    """A TorchScript archive whose only member is parameter "0" = tensor."""
    b = torch._C.ConcreteModuleTypeBuilder(Module)
    b.add_attribute("0", torch._C.TensorType.get(), True, False)
    m = torch._C._create_module_with_type(b.build().jit_type)
    m.setattr("0", tensor)
    m.save(path)


def fail(msg):
    sys.exit(f"export_pth: {msg}")


def decimal_cents(chunk):
    """Unscaled int64 values of a DECIMAL(p<=18, 2) chunk."""
    if chunk.type.scale != 2:
        fail(f"expected scale 2, got {chunk.type}")
    words = np.frombuffer(chunk.buffers()[1], dtype="<i8")[2 * chunk.offset: 2 * (chunk.offset + len(chunk))]
    lo, hi = words[0::2], words[1::2]
    if not np.array_equal(hi, lo >> 63):
        fail("decimal value exceeds int64")
    return lo


def fixed_width(chunk, width, col):
    """strncpy(dst, s, width) for every value: bytes, then NUL padding."""
    lens = pc.binary_length(chunk).to_numpy(zero_copy_only=False)
    if len(lens) and lens.max() > width:
        fail(f"{col}: value of {lens.max()} bytes exceeds width {width}")
    padded = pc.utf8_rpad(chunk, width=width, padding="\0")
    offsets = np.frombuffer(padded.buffers()[1], dtype="<i4")[padded.offset: padded.offset + len(padded) + 1]
    data = np.frombuffer(padded.buffers()[2], dtype=np.int8)[offsets[0]: offsets[-1]]
    if len(data) != len(padded) * width:
        fail(f"{col}: non-ASCII value")
    return data.reshape(len(padded), width)


def convert(col, chunk):
    """One parquet column chunk -> numpy array in TQP-Vortex's encoding."""
    if chunk.null_count:
        fail(f"{col}: {chunk.null_count} nulls")
    t = chunk.type
    if pa.types.is_integer(t):
        return chunk.to_numpy().astype(np.int64)
    if pa.types.is_decimal(t):
        cents = decimal_cents(chunk)
        if col == "l_quantity":
            if (cents % 100).any():
                fail("l_quantity has a fractional value")
            return cents // 100
        return cents.astype(np.float64) / 100.0
    if pa.types.is_date32(t):
        days = chunk.cast(pa.int32()).to_numpy()
        if len(days) and (days.min() < DATE_LO or days.max() > DATE_HI):
            fail(f"{col}: date outside 1992-1998")
        return (days - EPOCH).astype(np.int32)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        if col in CHAR1:
            codes = fixed_width(chunk, 1, col)
            if (codes == 0).any():
                fail(f"{col}: empty value")
            return codes[:, 0].copy()
        return fixed_width(chunk, WIDTH[col], col)
    fail(f"{col}: unexpected type {t}")


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    src, sf, out = sys.argv[1].rstrip("/"), int(sys.argv[2]), sys.argv[3]
    os.makedirs(out, exist_ok=True)
    tmp = os.path.join(out, f".SF{sf}.partial")
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    manifest = {"scale_factor": sf, "source": os.path.abspath(src),
                "row_order": "parquet order: part-files in sorted name order, row groups and rows as stored",
                "tables": {}, "columns": {}}
    for table in TABLES:
        files = sorted(glob.glob(f"{src}/{table}/*.parquet"))
        if not files:
            fail(f"no parquet files in {src}/{table}")
        pfs = [pq.ParquetFile(f) for f in files]
        rows = [p.metadata.num_rows for p in pfs]
        n = sum(rows)
        manifest["tables"][table] = {"rows": n, "files": [[os.path.basename(f), r] for f, r in zip(files, rows)]}
        for col in [c for c in pfs[0].schema_arrow.names if c not in SKIP]:
            parts = []
            for p in pfs:
                for chunk in p.read(columns=[col]).column(0).chunks:
                    parts.append(convert(col, chunk))
            arr = np.concatenate(parts) if parts else None
            if arr is None or len(arr) != n:
                fail(f"{col}: {0 if arr is None else len(arr)} rows, expected {n}")
            name = f"SF{sf}-tensor-{col.upper()}.pth"
            tensor = torch.from_numpy(np.ascontiguousarray(arr))
            save(tensor, os.path.join(tmp, name))
            manifest["columns"][col.upper()] = {"dtype": str(tensor.dtype).replace("torch.", ""),
                                                "shape": list(tensor.shape)}
            print(f"  {name:32s} {manifest['columns'][col.upper()]['dtype']:8s} {tuple(tensor.shape)}", flush=True)
            del parts, arr, tensor
    with open(os.path.join(tmp, f"SF{sf}-manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    if len(manifest["columns"]) != 54:
        fail(f"wrote {len(manifest['columns'])} columns, expected 54")
    for name in sorted(os.listdir(tmp)):
        os.replace(os.path.join(tmp, name), os.path.join(out, name))
    os.rmdir(tmp)
    print(f"exported SF{sf}: 54 columns + manifest -> {out}")


if __name__ == "__main__":
    main()
