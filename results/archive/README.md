# Archived results — mixed-provenance SF30/50/100/300

`all_results_sf30-300_mixed_provenance.csv` holds the SF30/50/100/300 rows that
were removed from `results/all_results.csv` on the provenance audit (see
`results/GENERATOR.md`).

They are **not deleted** because the numbers themselves are internally consistent
(all engines agree per SF; result row counts match). They were pulled out of the
canonical file because the parquet backing them was **not** the clean external
NDS-H generator that the rest of the suite requires:

| SF | backing parquet | writer | why disqualified |
|---:|-----------------|--------|------------------|
| 30 | `sf30_parquet` | cudf 24.10 | not the Spark/NDS-H transcode |
| 50 | `sf50_parquet` | DuckDB v1.5.4 | self-generated (forbidden) |
| 100 | `sf100_canon_parquet` | DuckDB v1.5.4 | self-generated; real NDS-H SF100 build is corrupt |
| 300 | `sf300_canon_parquet` | DuckDB v1.5.4 | self-generated; real NDS-H SF300 build is corrupt |

To restore these SFs to `all_results.csv`, regenerate each dataset with the clean
NDS-H pipeline, validate it (`results/validate_dataset.py`), and re-run every
engine — see the regeneration section of `results/GENERATOR.md`.
