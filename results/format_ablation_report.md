# SF100 parquet-format ablation: sum over q1-22 of per-query median cold seconds (min..max of per-round totals); [n] = failed queries excluded

| variant | duckdb_cpu | sirius | polars_gpu | rapids |
|---|---|---|---|---|
| shuffle-plain-snappy | 31.4 (31.2..32.5) | 38.4 (37.2..39.9) | 80.0 (78.6..81.8) | 663.8 (650.5..690.2) |
| shuffle-dict-snappy | 29.4 (28.7..36.7) | 35.0 (34.0..38.0) | 72.0 (71.3..73.3) | 665.8 (639.4..697.7) |
| shuffle-delta-snappy | 29.7 (28.7..39.0) | 31.8 (31.8..32.7) | 40.9 (40.8..41.5) | 682.1 (676.4..705.2) |
| shuffle-plain-zstd | 31.2 (30.7..31.7) | 34.5 (34.6..35.4) | 92.0 (91.0..94.9) | 657.2 (649.0..688.2) |
| shuffle-dict-zstd | 29.9 (28.1..31.2) | 32.6 (32.3..34.1) | 79.4 (79.6..80.5) | 676.8 (666.1..703.7) |
| shuffle-delta-zstd | 27.9 (27.1..28.9) | 30.9 (30.7..32.1) | 41.8 (41.6..43.6) | 332.5 (328.0..342.8) [8 FAIL] |
| shuffle-plain-lz4raw | 29.4 (28.7..29.9) | 39.7 (39.5..40.9) | 98.4 (98.3..99.4) | 700.9 (692.9..727.2) |
| shuffle-dict-lz4raw | 28.1 (28.0..28.6) | 36.0 (34.9..36.9) | 85.0 (84.0..86.9) | 678.8 (665.4..685.0) |
| shuffle-delta-lz4raw | 28.0 (27.7..29.4) | 31.4 (31.4..32.9) | 43.0 (42.8..44.5) | 646.3 (633.9..683.7) |
| keyorder-plain-snappy | 27.7 (25.8..28.8) | 36.0 (35.6..36.5) | 70.9 (70.6..72.1) | 643.0 (635.7..671.4) |
| keyorder-dict-snappy | 26.4 (27.0..27.7) | 34.6 (34.4..35.4) | 59.6 (58.8..61.2) | 655.6 (630.2..680.0) |
| keyorder-delta-snappy | 24.9 (24.7..25.6) | 29.9 (29.7..31.4) | 45.4 (45.8..46.4) | 681.9 (653.3..692.0) |

## duckdb_cpu: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 31.4 (1.07x) | 31.2 (1.06x) | 29.4 (1.00x) |
| dict | 29.4 (1.00x) | 29.9 (1.02x) | 28.1 (0.96x) |
| delta | 29.7 (1.01x) | 27.9 (0.95x) | 28.0 (0.95x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 27.7 | 0.88x |
| dict | 26.4 | 0.90x |
| delta | 24.9 | 0.84x |

## sirius: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 38.4 (1.10x) | 34.5 (0.99x) | 39.7 (1.14x) |
| dict | 35.0 (1.00x) | 32.6 (0.93x) | 36.0 (1.03x) |
| delta | 31.8 (0.91x) | 30.9 (0.88x) | 31.4 (0.90x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 36.0 | 0.94x |
| dict | 34.6 | 0.99x |
| delta | 29.9 | 0.94x |

## polars_gpu: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 80.0 (1.11x) | 92.0 (1.28x) | 98.4 (1.37x) |
| dict | 72.0 (1.00x) | 79.4 (1.10x) | 85.0 (1.18x) |
| delta | 40.9 (0.57x) | 41.8 (0.58x) | 43.0 (0.60x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 70.9 | 0.89x |
| dict | 59.6 | 0.83x |
| delta | 45.4 | 1.11x |

## rapids: shuffle layout, encoding x compression (total s, x = vs dict/snappy)

| encoding | snappy | zstd | lz4raw |
|---|---|---|---|
| plain | 663.8 (1.00x) | 657.2 (0.99x) | 700.9 (1.05x) |
| dict | 665.8 (1.00x) | 676.8 (1.02x) | 678.8 (1.02x) |
| delta | 682.1 (1.02x) | 332.5 (0.50x) | 646.3 (0.97x) |

| encoding | keyorder-snappy | vs shuffle |
|---|---|---|
| plain | 643.0 | 0.97x |
| dict | 655.6 | 0.98x |
| delta | 681.9 | 1.00x |

## duckdb_cpu: per-query median cold seconds

| query | shuffle-plain-snappy | shuffle-dict-snappy | shuffle-delta-snappy | shuffle-plain-zstd | shuffle-dict-zstd | shuffle-delta-zstd | shuffle-plain-lz4raw | shuffle-dict-lz4raw | shuffle-delta-lz4raw | keyorder-plain-snappy | keyorder-dict-snappy | keyorder-delta-snappy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| query1 | 2.11 | 2.07 | 1.63 | 1.95 | 1.88 | 1.49 | 1.75 | 1.60 | 1.65 | 1.97 | 2.23 | 1.45 |
| query2 | 0.83 | 0.79 | 0.85 | 0.81 | 0.78 | 0.72 | 0.77 | 0.73 | 0.81 | 0.87 | 0.79 | 0.78 |
| query3 | 0.90 | 0.89 | 0.69 | 0.92 | 0.97 | 0.69 | 0.81 | 0.84 | 0.65 | 0.83 | 0.87 | 0.69 |
| query4 | 0.86 | 0.74 | 0.82 | 0.77 | 0.77 | 0.64 | 0.72 | 0.65 | 0.78 | 0.81 | 0.73 | 0.71 |
| query5 | 0.93 | 0.96 | 0.77 | 0.98 | 0.97 | 0.76 | 0.82 | 0.93 | 0.83 | 0.96 | 0.94 | 0.94 |
| query6 | 0.33 | 0.26 | 0.22 | 0.34 | 0.23 | 0.19 | 0.26 | 0.24 | 0.23 | 0.33 | 0.27 | 0.25 |
| query7 | 0.85 | 0.81 | 0.66 | 0.88 | 0.86 | 0.68 | 0.72 | 0.73 | 0.66 | 0.85 | 0.85 | 0.73 |
| query8 | 1.07 | 1.04 | 0.92 | 1.15 | 1.05 | 0.87 | 0.96 | 0.92 | 0.89 | 1.12 | 1.06 | 0.93 |
| query9 | 2.98 | 2.92 | 2.71 | 2.88 | 2.88 | 2.61 | 3.00 | 2.61 | 2.76 | 2.94 | 2.99 | 2.64 |
| query10 | 0.89 | 0.79 | 0.93 | 0.94 | 0.84 | 0.86 | 0.79 | 0.75 | 0.86 | 0.86 | 0.74 | 0.92 |
| query11 | 2.79 | 2.82 | 2.80 | 2.76 | 2.87 | 2.82 | 2.75 | 2.80 | 2.77 | 2.79 | 2.80 | 2.69 |
| query12 | 0.81 | 0.63 | 0.82 | 0.75 | 0.55 | 0.77 | 0.70 | 0.49 | 0.77 | 0.67 | 0.49 | 0.78 |
| query13 | 1.25 | 1.21 | 1.24 | 1.20 | 1.19 | 1.20 | 1.20 | 1.16 | 1.22 | 1.18 | 1.23 | 1.17 |
| query14 | 0.65 | 0.60 | 0.58 | 0.67 | 0.59 | 0.51 | 0.58 | 0.52 | 0.55 | 0.66 | 0.57 | 0.55 |
| query15 | 0.61 | 0.55 | 0.63 | 0.74 | 0.50 | 0.48 | 0.67 | 0.52 | 0.51 | 0.72 | 0.51 | 0.47 |
| query16 | 0.67 | 0.52 | 0.87 | 0.74 | 0.53 | 0.58 | 0.84 | 0.52 | 0.54 | 0.68 | 0.58 | 0.51 |
| query17 | 0.95 | 0.84 | 0.98 | 0.92 | 0.84 | 0.76 | 0.85 | 0.84 | 0.82 | 0.90 | 1.04 | 0.79 |
| query18 | 4.29 | 4.26 | 4.07 | 4.51 | 3.97 | 4.12 | 3.94 | 3.65 | 4.11 | 1.90 | 1.81 | 1.87 |
| query19 | 1.69 | 1.32 | 2.26 | 1.74 | 1.85 | 1.94 | 1.73 | 1.81 | 1.79 | 1.27 | 1.06 | 1.47 |
| query20 | 1.23 | 1.21 | 0.97 | 1.16 | 1.41 | 0.97 | 1.13 | 1.22 | 0.94 | 1.27 | 1.02 | 1.08 |
| query21 | 3.81 | 3.52 | 3.40 | 3.85 | 3.51 | 3.31 | 3.37 | 3.45 | 3.14 | 3.07 | 3.34 | 2.69 |
| query22 | 0.86 | 0.61 | 0.85 | 0.58 | 0.86 | 0.90 | 1.00 | 1.10 | 0.73 | 1.09 | 0.51 | 0.75 |

## sirius: per-query median cold seconds

| query | shuffle-plain-snappy | shuffle-dict-snappy | shuffle-delta-snappy | shuffle-plain-zstd | shuffle-dict-zstd | shuffle-delta-zstd | shuffle-plain-lz4raw | shuffle-dict-lz4raw | shuffle-delta-lz4raw | keyorder-plain-snappy | keyorder-dict-snappy | keyorder-delta-snappy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| query1 | 1.38 | 1.04 | 1.11 | 1.45 | 1.12 | 1.13 | 1.75 | 1.10 | 1.17 | 1.33 | 1.10 | 1.08 |
| query2 | 0.92 | 1.00 | 0.87 | 0.81 | 0.94 | 0.88 | 0.99 | 0.95 | 0.82 | 0.86 | 0.93 | 0.78 |
| query3 | 2.68 | 2.34 | 2.26 | 2.45 | 2.45 | 2.33 | 2.65 | 2.60 | 2.26 | 2.55 | 2.62 | 2.28 |
| query4 | 1.03 | 0.97 | 0.73 | 0.83 | 0.77 | 0.77 | 0.92 | 0.87 | 0.74 | 1.00 | 0.84 | 0.98 |
| query5 | 1.73 | 1.57 | 1.13 | 1.32 | 1.33 | 1.11 | 1.65 | 1.64 | 1.12 | 1.37 | 1.50 | 0.96 |
| query6 | 0.93 | 0.79 | 0.81 | 0.88 | 0.75 | 0.80 | 1.08 | 0.89 | 0.75 | 1.02 | 0.78 | 0.87 |
| query7 | 2.01 | 1.82 | 1.65 | 1.73 | 1.66 | 1.46 | 1.98 | 1.88 | 1.54 | 1.88 | 1.92 | 1.48 |
| query8 | 2.88 | 2.83 | 2.97 | 2.62 | 2.60 | 2.37 | 3.31 | 2.85 | 2.45 | 2.73 | 2.75 | 2.29 |
| query9 | 2.26 | 2.22 | 1.42 | 1.91 | 1.78 | 1.65 | 2.28 | 2.07 | 1.50 | 1.91 | 2.04 | 1.26 |
| query10 | 2.02 | 1.74 | 1.80 | 1.91 | 1.73 | 1.62 | 2.04 | 1.91 | 1.87 | 1.92 | 1.70 | 1.62 |
| query11 | 1.49 | 1.44 | 1.45 | 1.40 | 1.46 | 1.35 | 1.71 | 1.50 | 1.36 | 1.43 | 1.44 | 1.39 |
| query12 | 1.24 | 0.91 | 0.89 | 0.97 | 0.79 | 0.92 | 1.37 | 0.90 | 0.90 | 1.01 | 0.78 | 0.80 |
| query13 | 0.71 | 0.80 | 0.67 | 0.64 | 0.65 | 0.73 | 0.80 | 0.89 | 0.91 | 0.68 | 0.67 | 0.68 |
| query14 | 1.01 | 0.91 | 0.71 | 0.90 | 0.77 | 0.70 | 1.07 | 0.85 | 0.69 | 0.99 | 0.89 | 0.72 |
| query15 | 1.18 | 0.87 | 0.74 | 0.90 | 0.77 | 0.70 | 1.10 | 0.91 | 0.73 | 0.96 | 1.02 | 0.73 |
| query16 | 0.67 | 0.63 | 0.66 | 0.67 | 0.64 | 0.62 | 0.65 | 0.66 | 0.68 | 0.60 | 0.62 | 0.57 |
| query17 | 2.34 | 2.21 | 2.12 | 2.18 | 2.19 | 1.97 | 2.37 | 2.18 | 2.06 | 2.44 | 2.32 | 2.10 |
| query18 | 1.71 | 1.57 | 1.25 | 1.45 | 1.34 | 1.20 | 1.69 | 1.70 | 1.20 | 1.12 | 1.17 | 0.90 |
| query19 | 4.62 | 4.34 | 4.28 | 4.58 | 4.11 | 4.32 | 4.84 | 4.43 | 4.32 | 5.21 | 4.58 | 4.42 |
| query20 | 1.97 | 1.80 | 1.64 | 1.82 | 1.78 | 1.61 | 2.06 | 2.05 | 1.62 | 1.87 | 1.78 | 1.62 |
| query21 | 3.09 | 2.67 | 2.17 | 2.63 | 2.41 | 2.13 | 2.91 | 2.70 | 2.19 | 2.60 | 2.60 | 1.93 |
| query22 | 0.55 | 0.48 | 0.50 | 0.47 | 0.57 | 0.49 | 0.54 | 0.52 | 0.48 | 0.53 | 0.51 | 0.47 |

## polars_gpu: per-query median cold seconds

| query | shuffle-plain-snappy | shuffle-dict-snappy | shuffle-delta-snappy | shuffle-plain-zstd | shuffle-dict-zstd | shuffle-delta-zstd | shuffle-plain-lz4raw | shuffle-dict-lz4raw | shuffle-delta-lz4raw | keyorder-plain-snappy | keyorder-dict-snappy | keyorder-delta-snappy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| query1 | 3.36 | 2.47 | 2.06 | 3.70 | 2.71 | 1.94 | 3.98 | 2.64 | 2.27 | 3.01 | 2.43 | 2.45 |
| query2 | 0.69 | 0.64 | 0.60 | 0.77 | 0.78 | 0.57 | 0.77 | 0.75 | 0.60 | 0.63 | 0.61 | 0.52 |
| query3 | 4.57 | 3.82 | 1.97 | 5.47 | 4.16 | 2.13 | 5.78 | 4.45 | 1.98 | 3.73 | 3.30 | 2.29 |
| query4 | 1.94 | 1.64 | 1.22 | 2.27 | 1.88 | 1.16 | 2.44 | 2.02 | 1.21 | 1.39 | 1.21 | 1.00 |
| query5 | 2.27 | 2.28 | 1.59 | 2.98 | 2.75 | 1.53 | 3.83 | 3.25 | 1.45 | 2.21 | 2.55 | 1.47 |
| query6 | 1.27 | 1.09 | 0.89 | 1.64 | 1.32 | 0.81 | 1.95 | 1.27 | 0.78 | 1.25 | 1.10 | 0.71 |
| query7 | 9.82 | 8.97 | 4.67 | 11.14 | 9.45 | 4.67 | 10.86 | 9.89 | 4.75 | 7.84 | 6.18 | 5.00 |
| query8 | 3.02 | 3.01 | 1.83 | 3.96 | 3.59 | 1.80 | 4.91 | 4.60 | 1.88 | 2.94 | 3.52 | 1.80 |
| query9 | 14.24 | 13.96 | 5.71 | 15.48 | 15.16 | 5.99 | 15.02 | 14.49 | 6.01 | 11.80 | 9.92 | 6.57 |
| query10 | 6.43 | 6.32 | 3.61 | 7.50 | 6.59 | 3.60 | 7.33 | 7.18 | 3.83 | 6.41 | 5.46 | 5.01 |
| query11 | 0.56 | 0.53 | 0.43 | 0.63 | 0.57 | 0.53 | 0.66 | 0.62 | 0.42 | 0.54 | 0.51 | 0.45 |
| query12 | 1.78 | 1.22 | 1.09 | 2.24 | 1.53 | 1.15 | 2.72 | 1.71 | 1.34 | 1.68 | 1.31 | 1.08 |
| query13 | 1.58 | 1.60 | 1.79 | 1.95 | 1.96 | 2.08 | 3.35 | 2.73 | 2.96 | 1.59 | 1.58 | 1.81 |
| query14 | 1.88 | 1.58 | 1.27 | 2.31 | 1.78 | 1.27 | 2.72 | 2.14 | 1.28 | 1.89 | 1.56 | 1.25 |
| query15 | 1.21 | 1.06 | 0.88 | 1.65 | 1.46 | 0.83 | 1.96 | 1.68 | 0.82 | 1.16 | 1.15 | 0.77 |
| query16 | 0.97 | 0.95 | 0.69 | 1.00 | 0.79 | 0.72 | 0.99 | 0.83 | 0.76 | 0.91 | 0.67 | 0.68 |
| query17 | 1.88 | 1.74 | 0.85 | 2.04 | 1.85 | 0.94 | 2.18 | 2.10 | 0.81 | 1.72 | 1.66 | 0.82 |
| query18 | 2.83 | 2.69 | 1.43 | 3.19 | 2.88 | 1.39 | 3.36 | 3.27 | 1.33 | 1.80 | 1.90 | 1.62 |
| query19 | 4.71 | 3.97 | 2.59 | 5.71 | 4.55 | 2.91 | 6.56 | 4.57 | 3.02 | 4.50 | 3.87 | 2.64 |
| query20 | 2.56 | 2.17 | 1.78 | 2.89 | 2.71 | 1.51 | 3.26 | 2.96 | 1.57 | 2.31 | 2.16 | 1.41 |
| query21 | 12.03 | 9.91 | 3.55 | 13.01 | 10.42 | 3.89 | 13.32 | 11.41 | 3.54 | 11.09 | 6.54 | 5.71 |
| query22 | 0.45 | 0.42 | 0.38 | 0.48 | 0.47 | 0.39 | 0.47 | 0.46 | 0.42 | 0.50 | 0.43 | 0.36 |

## rapids: per-query median cold seconds

| query | shuffle-plain-snappy | shuffle-dict-snappy | shuffle-delta-snappy | shuffle-plain-zstd | shuffle-dict-zstd | shuffle-delta-zstd | shuffle-plain-lz4raw | shuffle-dict-lz4raw | shuffle-delta-lz4raw | keyorder-plain-snappy | keyorder-dict-snappy | keyorder-delta-snappy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| query1 | 8.95 | 9.90 | 9.97 | 10.13 | 9.93 | 9.39 | 10.21 | 10.03 | 9.00 | 9.87 | 9.12 | 8.77 |
| query2 | 35.27 | 40.07 | 39.36 | 34.38 | 34.43 | FAIL(0/3 ok) | 36.88 | 34.69 | 34.61 | 34.47 | 35.16 | 40.14 |
| query3 | 30.37 | 29.81 | 34.51 | 30.38 | 30.50 | 30.78 | 30.58 | 29.61 | 28.81 | 30.41 | 30.20 | 33.85 |
| query4 | 18.39 | 19.70 | 19.57 | 21.16 | 19.90 | 18.17 | 19.95 | 20.69 | 17.82 | 17.64 | 18.41 | 17.77 |
| query5 | 48.12 | 45.83 | 48.01 | 49.01 | 50.53 | FAIL(1/3 ok) | 49.82 | 48.51 | 49.73 | 52.20 | 54.42 | 51.99 |
| query6 | 5.58 | 6.64 | 5.63 | 5.82 | 6.42 | 5.72 | 6.96 | 6.90 | 5.55 | 6.12 | 6.70 | 5.70 |
| query7 | 34.98 | 40.13 | 34.80 | 33.53 | 35.01 | FAIL(1/3 ok) | 36.58 | 38.01 | 33.32 | 33.34 | 33.61 | 39.21 |
| query8 | 51.23 | 57.10 | 54.54 | 51.60 | 54.59 | FAIL(0/3 ok) | 57.12 | 51.02 | 51.30 | 50.98 | 53.90 | 54.72 |
| query9 | 58.93 | 60.82 | 64.79 | 61.50 | 62.67 | FAIL(0/3 ok) | 66.12 | 64.36 | 65.11 | 61.99 | 65.80 | 60.98 |
| query10 | 29.50 | 29.77 | 33.01 | 29.40 | 30.26 | FAIL(0/3 ok) | 31.41 | 33.46 | 31.04 | 30.05 | 29.57 | 35.07 |
| query11 | 43.46 | 37.46 | 43.32 | 37.06 | 43.11 | FAIL(2/3 ok) | 36.33 | 42.48 | 36.66 | 37.39 | 38.56 | 38.20 |
| query12 | 18.51 | 17.86 | 16.67 | 18.22 | 20.85 | 17.34 | 18.75 | 18.24 | 17.33 | 16.88 | 17.18 | 18.39 |
| query13 | 18.48 | 17.43 | 16.07 | 16.23 | 16.69 | 17.49 | 19.09 | 18.14 | 18.27 | 16.13 | 16.79 | 17.34 |
| query14 | 20.22 | 20.72 | 20.04 | 20.49 | 20.50 | 20.22 | 20.67 | 21.00 | 19.59 | 21.75 | 20.16 | 19.16 |
| query15 | 32.09 | 33.01 | 37.28 | 32.90 | 33.95 | 39.37 | 39.86 | 39.07 | 32.06 | 33.36 | 34.20 | 33.12 |
| query16 | 32.02 | 31.40 | 33.33 | 32.39 | 36.12 | 32.51 | 33.02 | 32.25 | 32.51 | 32.26 | 31.39 | 37.64 |
| query17 | 26.89 | 28.59 | 26.50 | 31.34 | 27.82 | 27.54 | 31.96 | 27.28 | 25.45 | 28.17 | 26.01 | 24.74 |
| query18 | 44.88 | 38.78 | 39.82 | 40.22 | 40.33 | 38.60 | 47.63 | 39.14 | 38.90 | 33.00 | 35.67 | 39.82 |
| query19 | 14.47 | 14.50 | 14.32 | 15.45 | 14.43 | 14.29 | 18.27 | 15.90 | 15.16 | 14.53 | 15.51 | 14.64 |
| query20 | 26.91 | 23.89 | 27.36 | 24.35 | 27.32 | FAIL(0/3 ok) | 25.75 | 25.86 | 24.79 | 24.94 | 24.52 | 26.80 |
| query21 | 41.23 | 40.98 | 44.45 | 41.52 | 42.42 | 42.36 | 45.56 | 42.92 | 40.50 | 38.84 | 39.27 | 43.94 |
| query22 | 23.34 | 21.36 | 18.71 | 20.16 | 19.02 | 18.73 | 18.39 | 19.27 | 18.81 | 18.65 | 19.48 | 19.90 |

## on-disk size per variant (GiB)

| variant | total | lineitem |
|---|---|---|
| shuffle-plain-snappy | 45.5 | 32.0 |
| shuffle-dict-snappy | 35.4 | 22.9 |
| shuffle-delta-snappy | 31.8 | 21.1 |
| shuffle-plain-zstd | 27.6 | 19.0 |
| shuffle-dict-zstd | 24.8 | 16.5 |
| shuffle-delta-zstd | 25.7 | 17.6 |
| shuffle-plain-lz4raw | 47.9 | 33.8 |
| shuffle-dict-lz4raw | 35.9 | 23.0 |
| shuffle-delta-lz4raw | 33.2 | 21.6 |
| keyorder-plain-snappy | 42.0 | 28.9 |
| keyorder-dict-snappy | 33.7 | 21.6 |
| keyorder-delta-snappy | 29.3 | 19.4 |

## Per-column size analysis (SF100): where the bytes move, and whether bytes predict time

Data: `format_ablation_colsizes.csv` (compressed/encoded bytes and encodings per variant x
table x column, summed from every parquet footer by `format_ablation_colsizes.py`);
tables A-D below are produced by `format_ablation_columns.py`.

- **Encoding movers.** Everything that moves is in lineitem. The four PLAIN-fallback INT64
  columns (l_suppkey, l_orderkey, l_partkey, l_extendedprice; 2.8-3.1 GB each under
  plain/dict) drop to 0.53-0.60x with DELTA_BINARY_PACKED (~1.25 GB saved each). The
  low-cardinality columns move the other way: dictionary takes l_shipinstruct / l_shipmode /
  l_linestatus / l_returnflag to 0.12-0.19x of plain, while DELTA_BYTE_ARRAY on the same
  columns is 3-10x *larger* than dictionary (l_shipinstruct 146 MB -> 1446 MB). Dates are a
  tie (dict 0.48x of plain, delta 0.50x). Comments (l_/o_/ps_comment, ~13.4 GB, 38% of the
  dict/snappy dataset) barely move with encoding (delta 0.92-0.97x) and are mostly not read
  by the queries.
- **Best encoding per column** (compressed, snappy): every high-cardinality number is best
  under delta (19/19); every dictionary-encodable column is best under dict (21/24, the 3
  exceptions are ties within 5%); high-cardinality strings are best under delta by 5-10%
  (prefix sharing) except phones (plain). PLAIN never wins outright. A per-column mix would
  be 27.6 GiB vs the best uniform variant, 31.8 GiB (delta) / 35.4 GiB (dict).
- **Key order** only compresses the sort keys, but dramatically: l_orderkey delta
  1474 -> 182 MB, o_orderkey 367 -> 5 MB, ps_partkey 149 -> 1 MB; dictionary also starts
  fitting for l_orderkey (0.53x). Whole dataset only 0.92-0.95x because comments dominate.
- **Codecs are not encoding-neutral.** Ratio compressed/encoded: plain 0.41 (snappy) /
  0.25 (zstd), dict 0.52 / 0.36, delta 0.53 / 0.42. The codec substitutes for the encoding
  rather than stacking on it: on dictionary-able columns plain+zstd reaches 0.16 but still
  lands at 7.5 GiB vs 4.7 GiB for dict, which the codec cannot shrink further (1.00 snappy,
  0.96 zstd). Delta output is the least compressible (bit-packed deltas), so delta+zstd
  (25.6 GiB) ends up larger than dict+zstd (24.8 GiB) although delta+snappy is smaller than
  dict+snappy. lz4raw ~= snappy (+3-5%). With zstd all encodings land within
  24.8-27.6 GiB; with snappy they spread 31.8-45.4 GiB.
- **Size vs time.** Within a query, across the 12 variants, bytes of the touched columns
  predict median seconds for Sirius (mean r 0.78, 18/22 queries r>0.5) and polars_gpu
  (0.60, 17/22) but not DuckDB (0.30) or RAPIDS (0.05). Per factor: Sirius responds to
  both codec bytes (r 0.50) and encoding bytes (0.75), i.e. it is byte-bound (~0.34 s per
  GiB of dataset); polars_gpu responds only to encoding bytes (0.87), not codec bytes
  (-0.07), so zstd's smaller files do not help it (GPU zstd decompression plus the
  streaming-partition effect dominate); DuckDB's CPU decompression cost cancels the byte
  saving (codec r -0.12; lz4 is its fastest codec); RAPIDS is flat under every factor
  (Spark overhead). Whole-dataset size is a weak predictor everywhere because 38% of the
  bytes are comment columns the queries never read.

### A. Per-column compressed size by encoding (shuffle layout, snappy), sorted by bytes moved plain->best

| table | column | type | plain MB | dict MB | delta MB | best | dict/plain | delta/dict | MB saved plain->best | dict variant used |
|---|---|---|---|---|---|---|---|---|---|---|
| lineitem | l_suppkey | INT64 | 2825 | 2825 | 1535 | delta | 1.00 | 0.54 | 1290 | PLAIN-fallback |
| lineitem | l_orderkey | INT64 | 2761 | 2761 | 1474 | delta | 1.00 | 0.53 | 1287 | PLAIN-fallback |
| lineitem | l_partkey | INT64 | 3117 | 3117 | 1863 | delta | 1.00 | 0.60 | 1254 | PLAIN-fallback |
| lineitem | l_extendedprice | INT64 | 2960 | 2960 | 1758 | delta | 1.00 | 0.59 | 1201 | PLAIN-fallback |
| lineitem | l_shipinstruct | BYTE_ARRAY | 1228 | 146 | 1446 | dict | 0.12 | 9.93 | 1082 | dict |
| lineitem | l_shipmode | BYTE_ARRAY | 1283 | 217 | 1163 | dict | 0.17 | 5.36 | 1066 | dict |
| lineitem | l_shipdate | INT32 | 1836 | 876 | 919 | dict | 0.48 | 1.05 | 960 | dict |
| lineitem | l_receiptdate | INT32 | 1836 | 876 | 920 | dict | 0.48 | 1.05 | 960 | dict |
| lineitem | l_commitdate | INT32 | 1834 | 876 | 919 | dict | 0.48 | 1.05 | 958 | dict |
| lineitem | l_discount | INT64 | 1091 | 289 | 377 | dict | 0.26 | 1.31 | 803 | dict |
| lineitem | l_tax | INT64 | 1077 | 289 | 330 | dict | 0.27 | 1.14 | 789 | dict |
| lineitem | l_linenumber | INT32 | 957 | 217 | 307 | dict | 0.23 | 1.42 | 740 | dict |
| lineitem | l_quantity | INT64 | 1148 | 432 | 1005 | dict | 0.38 | 2.33 | 716 | dict |
| lineitem | l_returnflag | BYTE_ARRAY | 759 | 146 | 438 | dict | 0.19 | 3.01 | 613 | dict |
| lineitem | l_linestatus | BYTE_ARRAY | 646 | 75 | 317 | dict | 0.12 | 4.24 | 571 | dict |
| lineitem | l_comment | BYTE_ARRAY | 7359 | 7359 | 6792 | delta | 1.00 | 0.92 | 567 | PLAIN-fallback |
| orders | o_orderkey | INT64 | 700 | 700 | 367 | delta | 1.00 | 0.53 | 332 | PLAIN-fallback |
| orders | o_totalprice | INT64 | 802 | 802 | 475 | delta | 1.00 | 0.59 | 326 | PLAIN-fallback |
| orders | o_custkey | INT64 | 765 | 765 | 456 | delta | 1.00 | 0.60 | 308 | PLAIN-fallback |
| orders | o_orderpriority | BYTE_ARRAY | 326 | 54 | 356 | dict | 0.17 | 6.55 | 272 | dict |
| orders | o_clerk | BYTE_ARRAY | 914 | 914 | 663 | delta | 1.00 | 0.73 | 250 | PLAIN-fallback |
| orders | o_orderdate | INT32 | 458 | 230 | 230 | delta | 0.50 | 1.00 | 228 | dict |
| orders | o_comment | BYTE_ARRAY | 2836 | 2836 | 2626 | delta | 1.00 | 0.93 | 210 | PLAIN-fallback |
| partsupp | ps_partkey | INT64 | 320 | 320 | 149 | delta | 1.00 | 0.47 | 170 | PLAIN-fallback |
| partsupp | ps_supplycost | INT64 | 343 | 343 | 176 | delta | 1.00 | 0.51 | 167 | PLAIN-fallback |
| partsupp | ps_availqty | INT32 | 300 | 195 | 144 | delta | 0.65 | 0.74 | 156 | dict |
| partsupp | ps_suppkey | INT64 | 348 | 348 | 205 | delta | 1.00 | 0.59 | 144 | PLAIN-fallback |
| orders | o_orderstatus | BYTE_ARRAY | 167 | 36 | 89 | dict | 0.22 | 2.45 | 131 | dict |
| part | p_type | BYTE_ARRAY | 125 | 21 | 122 | dict | 0.17 | 5.83 | 104 | dict |
| partsupp | ps_comment | BYTE_ARRAY | 3248 | 3248 | 3166 | delta | 1.00 | 0.97 | 82 | PLAIN-fallback |
| part | p_container | BYTE_ARRAY | 66 | 15 | 65 | dict | 0.23 | 4.37 | 51 | dict |
| part | p_partkey | INT64 | 87 | 87 | 41 | delta | 1.00 | 0.47 | 46 | PLAIN-fallback |
| part | p_brand | BYTE_ARRAY | 53 | 12 | 31 | dict | 0.23 | 2.50 | 41 | dict |
| part | p_retailprice | INT64 | 84 | 84 | 44 | delta | 1.00 | 0.52 | 40 | PLAIN-fallback |
| customer | c_address | BYTE_ARRAY | 409 | 409 | 370 | delta | 1.00 | 0.90 | 39 | PLAIN-fallback |
| part | p_mfgr | BYTE_ARRAY | 46 | 7 | 16 | dict | 0.16 | 2.23 | 39 | dict |
| customer | c_custkey | INT64 | 66 | 66 | 31 | delta | 1.00 | 0.47 | 35 | PLAIN-fallback |
| customer | c_acctbal | INT64 | 71 | 71 | 38 | delta | 1.00 | 0.54 | 32 | PLAIN-fallback |
| customer | c_mktsegment | BYTE_ARRAY | 36 | 6 | 34 | dict | 0.16 | 6.08 | 30 | dict |
| orders | o_shippriority | INT32 | 27 | 0 | 1 | dict | 0.01 | 1.74 | 27 | dict |
| customer | c_name | BYTE_ARRAY | 94 | 94 | 70 | delta | 1.00 | 0.74 | 25 | PLAIN-fallback |
| part | p_size | INT32 | 38 | 15 | 18 | dict | 0.39 | 1.19 | 23 | dict |
| customer | c_comment | BYTE_ARRAY | 389 | 389 | 369 | delta | 1.00 | 0.95 | 20 | PLAIN-fallback |
| customer | c_nationkey | INT64 | 28 | 9 | 11 | dict | 0.33 | 1.23 | 19 | dict |
| part | p_name | BYTE_ARRAY | 255 | 255 | 242 | delta | 1.00 | 0.95 | 13 | PLAIN-fallback |
| part | p_comment | BYTE_ARRAY | 163 | 163 | 152 | delta | 1.00 | 0.93 | 11 | PLAIN-fallback |
| supplier | s_address | BYTE_ARRAY | 28 | 28 | 25 | delta | 1.00 | 0.90 | 3 | PLAIN-fallback |
| supplier | s_suppkey | INT64 | 4 | 4 | 2 | delta | 1.00 | 0.49 | 2 | PLAIN-fallback |
| supplier | s_acctbal | INT64 | 5 | 5 | 3 | delta | 1.00 | 0.56 | 2 | PLAIN-fallback |
| supplier | s_name | BYTE_ARRAY | 7 | 7 | 5 | delta | 1.00 | 0.69 | 2 | PLAIN-fallback |
| supplier | s_nationkey | INT64 | 2 | 1 | 1 | delta | 0.43 | 0.96 | 1 | dict |
| supplier | s_comment | BYTE_ARRAY | 24 | 24 | 23 | delta | 1.00 | 0.97 | 1 | PLAIN-fallback |
| nation | n_regionkey | INT64 | 0 | 0 | 0 | delta | 1.00 | 0.77 | 0 | PLAIN-fallback |
| nation | n_nationkey | INT64 | 0 | 0 | 0 | delta | 1.00 | 0.77 | 0 | PLAIN-fallback |
| region | r_regionkey | INT64 | 0 | 0 | 0 | delta | 1.00 | 0.77 | 0 | PLAIN-fallback |
| nation | n_comment | BYTE_ARRAY | 0 | 0 | 0 | delta | 1.00 | 1.00 | 0 | PLAIN-fallback |
| region | r_comment | BYTE_ARRAY | 0 | 0 | 0 | delta | 1.00 | 0.98 | 0 | PLAIN-fallback |
| nation | n_name | BYTE_ARRAY | 0 | 0 | 0 | delta | 1.00 | 1.00 | 0 | PLAIN-fallback |
| region | r_name | BYTE_ARRAY | 0 | 0 | 0 | delta | 1.00 | 1.00 | 0 | PLAIN-fallback |
| supplier | s_phone | BYTE_ARRAY | 12 | 12 | 13 | dict | 1.00 | 1.04 | 0 | PLAIN-fallback |
| customer | c_phone | BYTE_ARRAY | 170 | 170 | 175 | dict | 1.00 | 1.03 | 0 | PLAIN-fallback |

### B. Best encoding per column (compressed, snappy, shuffle) -- counts by physical type and by dictionary applicability

| type | dict variant | best=plain | best=dict | best=delta |
|---|---|---|---|---|
| BYTE_ARRAY | dict-encoded | 0 | 11 | 0 |
| BYTE_ARRAY | dict-fallback-to-PLAIN | 0 | 2 | 16 |
| INT32 | dict-encoded | 0 | 6 | 2 |
| INT64 | dict-encoded | 0 | 4 | 1 |
| INT64 | dict-fallback-to-PLAIN | 0 | 0 | 19 |

Totals by encoding (shuffle, snappy), GiB, and sum of per-column best:
| encoding | GiB |
|---|---|
| plain | 45.44 |
| dict | 35.36 |
| delta | 31.80 |
| per-column best (mixed) | 27.63 |

Keyorder vs shuffle (snappy) per encoding, GiB:
| encoding | shuffle | keyorder | ratio |
|---|---|---|---|
| delta | 31.80 | 29.26 | 0.92 |
| dict | 35.36 | 33.67 | 0.95 |
| plain | 45.44 | 41.95 | 0.92 |

Keyorder biggest per-column changes vs shuffle (dict/snappy), MB:
| table | column | enc | shuffle MB | keyorder MB | ratio |
|---|---|---|---|---|---|
| lineitem | l_orderkey | plain | 2761 | 893 | 0.32 |
| lineitem | l_orderkey | dict | 2761 | 1462 | 0.53 |
| lineitem | l_orderkey | delta | 1474 | 182 | 0.12 |
| lineitem | l_linenumber | plain | 957 | 454 | 0.47 |
| orders | o_orderkey | delta | 367 | 5 | 0.01 |
| lineitem | l_linestatus | plain | 646 | 333 | 0.52 |
| lineitem | l_returnflag | plain | 759 | 541 | 0.71 |
| partsupp | ps_suppkey | delta | 205 | 9 | 0.05 |
| partsupp | ps_partkey | plain | 320 | 134 | 0.42 |
| partsupp | ps_partkey | delta | 149 | 1 | 0.01 |
| lineitem | l_linenumber | delta | 307 | 169 | 0.55 |
| orders | o_orderkey | plain | 700 | 571 | 0.82 |

### C. Codec effect per encoding (shuffle). 'encoded' = uncompressed page bytes after encoding; ratio = compressed/encoded

| encoding | encoded GiB | snappy GiB | zstd GiB | lz4raw GiB | snappy ratio | zstd ratio | lz4 ratio |
|---|---|---|---|---|---|---|---|
| delta | 60.42 | 31.80 | 25.61 | 33.16 | 0.53 | 0.42 | 0.55 |
| dict | 68.21 | 35.36 | 24.80 | 35.88 | 0.52 | 0.36 | 0.53 |
| plain | 110.26 | 45.44 | 27.57 | 47.85 | 0.41 | 0.25 | 0.43 |

Per-column codec ratio (compressed/encoded), snappy vs zstd, by encoding -- columns where zstd gains most over snappy:
| table | column | enc | encoded MB | snappy | zstd | lz4raw | zstd/snappy |
|---|---|---|---|---|---|---|---|
| lineitem | l_comment | plain | 17454 | 0.42 | 0.28 | 0.45 | 0.67 |
| lineitem | l_comment | dict | 17454 | 0.42 | 0.28 | 0.45 | 0.67 |
| lineitem | l_comment | delta | 15783 | 0.43 | 0.29 | 0.44 | 0.67 |
| lineitem | l_extendedprice | dict | 4579 | 0.65 | 0.41 | 0.63 | 0.64 |
| lineitem | l_extendedprice | plain | 4579 | 0.65 | 0.41 | 0.63 | 0.64 |
| partsupp | ps_comment | delta | 9529 | 0.33 | 0.22 | 0.37 | 0.67 |
| lineitem | l_partkey | dict | 4579 | 0.68 | 0.46 | 0.65 | 0.67 |
| lineitem | l_partkey | plain | 4579 | 0.68 | 0.46 | 0.65 | 0.67 |
| partsupp | ps_comment | plain | 9728 | 0.33 | 0.23 | 0.37 | 0.69 |
| partsupp | ps_comment | dict | 9728 | 0.33 | 0.23 | 0.37 | 0.69 |
| lineitem | l_orderkey | plain | 4579 | 0.60 | 0.38 | 0.58 | 0.64 |
| lineitem | l_orderkey | dict | 4579 | 0.60 | 0.38 | 0.58 | 0.64 |
| lineitem | l_suppkey | plain | 4579 | 0.62 | 0.42 | 0.59 | 0.67 |
| lineitem | l_suppkey | dict | 4579 | 0.62 | 0.42 | 0.59 | 0.67 |
| orders | o_comment | dict | 7511 | 0.38 | 0.26 | 0.41 | 0.68 |

Codec ratio by (encoding, dictionary applicability) class, summed bytes:
| encoding | column class | encoded GiB | snappy ratio | zstd ratio | lz4 ratio |
|---|---|---|---|---|---|
| delta | dict-able | 16.89 | 0.54 | 0.45 | 0.56 |
| dict | dict-able | 4.92 | 1.00 | 0.96 | 1.00 |
| plain | dict-able | 46.98 | 0.32 | 0.16 | 0.36 |
| delta | high-card (PLAIN fallback) | 43.54 | 0.52 | 0.42 | 0.55 |
| dict | high-card (PLAIN fallback) | 63.28 | 0.48 | 0.32 | 0.49 |
| plain | high-card (PLAIN fallback) | 63.28 | 0.48 | 0.32 | 0.49 |

### D. Size vs execution time

query1: 7 cols
query2: 19 cols
query3: 10 cols
query4: 6 cols
query5: 16 cols
query6: 4 cols
query7: 13 cols
query8: 19 cols
query9: 17 cols
query10: 16 cols
query11: 8 cols
query12: 7 cols
query13: 4 cols
query14: 6 cols
query15: 8 cols
query16: 8 cols
query17: 6 cols
query18: 8 cols
query19: 10 cols
query20: 15 cols
query21: 11 cols
query22: 4 cols

Variant-level: total dataset GiB vs sum of per-query medians (all 12 variants; rapids delta-zstd excluded due to timeouts):
| engine | n variants | pearson | spearman | slope s/GiB |
|---|---|---|---|---|
| duckdb_cpu | 12 | 0.19 | 0.01 | 0.05 |
| sirius | 12 | 0.85 | 0.84 | 0.34 |
| polars_gpu | 12 | 0.45 | 0.43 | 1.26 |
| rapids | 11 | 0.11 | 0.00 | 0.25 |

Query-level: bytes of columns the query touches vs median seconds. within-query = mean Pearson across the 12 variants per query (isolates format effect); pooled = over all (query,variant) points:
| engine | queries | mean within-q r | median within-q r | queries r>0.5 | pooled pearson | pooled spearman | median slope s/GiB |
|---|---|---|---|---|---|---|---|
| duckdb_cpu | 22 | 0.30 | 0.33 | 3/22 | 0.09 | 0.17 | 0.02 |
| sirius | 22 | 0.78 | 0.82 | 18/22 | 0.55 | 0.68 | 0.08 |
| polars_gpu | 22 | 0.60 | 0.64 | 17/22 | 0.65 | 0.77 | 0.22 |
| rapids | 22 | 0.05 | 0.06 | 2/22 | 0.32 | 0.21 | 0.06 |

Same, restricted to shuffle layout and separately per factor (codec-only: fix encoding, vary codec; encoding-only: fix codec=snappy, vary encoding):
| engine | factor | n (query x group) | mean r | r>0.5 |
|---|---|---|---|---|
| duckdb_cpu | codec only (3 codecs x 3 enc, within enc) | 66 | -0.12 | 15/66 |
| duckdb_cpu | encoding only (snappy, shuffle) | 22 | 0.37 | 11/22 |
| sirius | codec only (3 codecs x 3 enc, within enc) | 66 | 0.50 | 43/66 |
| sirius | encoding only (snappy, shuffle) | 22 | 0.75 | 18/22 |
| polars_gpu | codec only (3 codecs x 3 enc, within enc) | 66 | -0.07 | 9/66 |
| polars_gpu | encoding only (snappy, shuffle) | 22 | 0.87 | 21/22 |
| rapids | codec only (3 codecs x 3 enc, within enc) | 58 | 0.09 | 18/58 |
| rapids | encoding only (snappy, shuffle) | 22 | -0.22 | 4/22 |

Per-query: touched bytes (dict-snappy) and seconds; ratio delta/dict for bytes and for each engine's time (shuffle snappy):
| query | dict GiB touched | plain/dict bytes | delta/dict bytes | duckdb_cpu (dict s, plain x, delta x) | sirius (dict s, plain x, delta x) | polars_gpu (dict s, plain x, delta x) | rapids (dict s, plain x, delta x) |
|---|---|---|---|---|---|---|---|
| query1 | 4.95 | 1.88 | 1.02 | 2.07s p1.02 d0.79 | 1.04s p1.32 d1.07 | 2.47s p1.36 d0.84 | 9.90s p0.90 d1.01 |
| query2 | 1.19 | 1.14 | 0.65 | 0.79s p1.05 d1.08 | 1.00s p0.92 d0.87 | 0.64s p1.09 d0.94 | 40.07s p0.88 d0.98 |
| query3 | 8.45 | 1.24 | 0.65 | 0.89s p1.01 d0.78 | 2.34s p1.15 d0.97 | 3.82s p1.20 d0.52 | 29.81s p1.02 d1.16 |
| query4 | 5.37 | 1.44 | 0.78 | 0.74s p1.15 d1.10 | 0.97s p1.07 d0.76 | 1.64s p1.18 d0.74 | 19.70s p0.93 d0.99 |
| query5 | 10.36 | 1.10 | 0.59 | 0.96s p0.96 d0.80 | 1.57s p1.10 d0.72 | 2.28s p0.99 d0.69 | 45.83s p1.05 d1.05 |
| query6 | 4.45 | 1.54 | 0.89 | 0.26s p1.26 d0.84 | 0.79s p1.18 d1.03 | 1.09s p1.16 d0.81 | 6.64s p0.84 d0.85 |
| query7 | 10.99 | 1.16 | 0.62 | 0.81s p1.05 d0.81 | 1.82s p1.10 d0.91 | 8.97s p1.09 d0.52 | 40.13s p0.87 d0.87 |
| query8 | 13.51 | 1.08 | 0.60 | 1.04s p1.03 d0.88 | 2.83s p1.02 d1.05 | 3.01s p1.00 d0.61 | 57.10s p0.90 d0.96 |
| query9 | 14.33 | 1.12 | 0.64 | 2.92s p1.02 d0.93 | 2.22s p1.02 d0.64 | 13.96s p1.02 d0.41 | 60.82s p0.97 d1.07 |
| query10 | 8.84 | 1.18 | 0.68 | 0.79s p1.13 d1.18 | 1.74s p1.16 d1.04 | 6.32s p1.02 d0.57 | 29.77s p0.99 d1.11 |
| query11 | 1.18 | 1.09 | 0.56 | 2.82s p0.99 d0.99 | 1.44s p1.03 d1.01 | 0.53s p1.05 d0.81 | 37.46s p1.16 d1.16 |
| query12 | 6.21 | 1.66 | 0.96 | 0.63s p1.29 d1.32 | 0.91s p1.37 d0.98 | 1.22s p1.45 d0.89 | 17.86s p1.04 d0.93 |
| query13 | 4.26 | 1.00 | 0.80 | 1.21s p1.03 d1.02 | 0.80s p0.89 d0.84 | 1.60s p0.98 d1.11 | 17.43s p1.06 d0.92 |
| query14 | 7.18 | 1.25 | 0.69 | 0.60s p1.08 d0.97 | 0.91s p1.11 d0.78 | 1.58s p1.19 d0.80 | 20.72s p0.98 d0.97 |
| query15 | 6.84 | 1.25 | 0.66 | 0.55s p1.11 d1.14 | 0.87s p1.35 d0.85 | 1.06s p1.14 d0.83 | 33.01s p0.97 d1.13 |
| query16 | 0.81 | 1.20 | 0.71 | 0.52s p1.28 d1.66 | 0.63s p1.06 d1.05 | 0.95s p1.03 d0.73 | 31.40s p1.02 d1.06 |
| query17 | 6.47 | 1.12 | 0.72 | 0.84s p1.12 d1.17 | 2.21s p1.06 d0.96 | 1.74s p1.08 d0.49 | 28.59s p0.94 d0.93 |
| query18 | 5.71 | 1.16 | 0.70 | 4.26s p1.01 d0.95 | 1.57s p1.09 d0.80 | 2.69s p1.05 d0.53 | 38.78s p1.16 d1.03 |
| query19 | 7.12 | 1.52 | 1.07 | 1.32s p1.27 d1.71 | 4.34s p1.06 d0.99 | 3.97s p1.19 d0.65 | 14.50s p1.00 d0.99 |
| query20 | 8.30 | 1.21 | 0.72 | 1.21s p1.01 d0.80 | 1.80s p1.09 d0.91 | 2.17s p1.18 d0.82 | 23.89s p1.13 d1.15 |
| query21 | 7.90 | 1.25 | 0.66 | 3.52s p1.09 d0.97 | 2.67s p1.16 d0.81 | 9.91s p1.21 d0.36 | 40.98s p1.01 d1.08 |
| query22 | 1.05 | 1.00 | 0.65 | 0.61s p1.41 d1.41 | 0.48s p1.14 d1.04 | 0.42s p1.07 d0.91 | 21.36s p1.09 d0.88 |


