# DuckDB CPU Baseline

Phases
- Add duckdb_cpu runner
    - parquet input; use TPC-H dataset same as DPFProto/TQP
    - just run and match expected results
- benchmarking
    - warm up each query
    - run each multiple times and record results
- CPU vs. GPU
    - get runtimes CPU (DuckDB)
    - get runtime GPU (GOLAP)
    - remember to collect the hardware used info

Paper summary
- Runtime comparison table
- Speedup table
- CPU vs. GPU cost table
- Figures for the Evaluation section


## Baseline Running
Minimal repeated-run DuckDB baseline over the shared TPC-H parquet dataset.

```bash
# build parquet from existing DPFProto tbl data
./duckdb/make_parquet.sh 100

# GOLAP/DPFProto subset, 1 warmup + 5 measured runs each
TPCH_SF=100 ./duckdb/run_duckdb.sh

# explicit parquet path and query list
TPCH_PARQUET=results/parquet RUNS=5 WARMUPS=1 ./duckdb/run_duckdb.sh "1 6 9"
```

Output defaults to `results/duckdb_runs.csv`:

```csv
engine,scale_factor,query,run,status,seconds,rows_or_error
duckdb_cpu,500,query1,1,OK,0.007805,1
```

The default query set matches GOLAP/DPFProto: q1, q3, q5, q6, q13, q16. The
runner uses `results/queries/stream_qualification.sql`, creates DuckDB views over
`<parquet>/<table>/*.parquet`, warms each query, then records every measured run.


## Getting Parquets

```bash
./rapids/nds_h_pipeline.sh 1 2 1 /dev/shm/tpch_sf1


```
