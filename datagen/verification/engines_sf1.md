# Engine verification: upstream vs datagen

## duckdb_cpu
  query    status A/B         rows A     rows B    secs A    secs B
  query1   OK/OK                   4          4     0.015     0.014
  query2   OK/OK                 100        100     0.013     0.013
  query3   OK/OK                  10         10     0.021     0.018
  query4   OK/OK                   5          5     0.023     0.019
  query5   OK/OK                   5          5     0.019     0.019
  query6   OK/OK                   1          1     0.004     0.005
  query7   OK/OK                   4          4     0.023     0.020
  query8   OK/OK                   2          2     0.022     0.020
  query9   OK/OK                 175        175     0.049     0.055
  query10  OK/OK                  20         20     0.040     0.046
  query11  OK/OK               29531      29531     0.059     0.057
  query12  OK/OK                   2          2     0.020     0.019
  query13  OK/OK                  42         42     0.037     0.037
  query14  OK/OK                   1          1     0.020     0.019
  query15  OK/OK                   1          1     0.011     0.013
  query16  OK/OK               18314      18314     0.034     0.045
  query17  OK/OK                   1          1     0.019     0.022
  query18  OK/OK                  57         57     0.044     0.049
  query19  OK/OK                   1          1     0.032     0.040
  query20  OK/OK                 186        186     0.019     0.020
  query21  OK/OK                 100        100     0.073     0.074
  query22  OK/OK                   7          7     0.021     0.029

## polars_gpu
  query    status A/B         rows A     rows B    secs A    secs B
  query1   OK/OK                   4          4     0.253     0.395
  query2   OK/OK                 100        100     0.333     0.382
  query3   OK/OK                  10         10     0.442     0.353
  query4   OK/OK                   5          5     0.561     0.475
  query5   OK/OK                   5          5     0.389     0.437
  query6   OK/OK                   1          1     0.209     0.254
  query7   OK/OK                   4          4     0.605     0.622
  query8   OK/OK                   2          2     0.453     0.400
  query9   OK/OK                 175        175     0.484     0.429
  query10  OK/OK                  20         20     0.529     0.583
  query11  OK/OK               29531      29531     0.299     0.240
  query12  OK/OK                   2          2     0.356     0.432
  query13  OK/OK                  42         42     0.299     0.339
  query14  OK/OK                   1          1     0.390     0.347
  query15  OK/OK                   1          1     0.362     0.349
  query16  OK/OK               18314      18314     0.392     0.528
  query17  OK/OK                   1          1     0.468     0.332
  query18  OK/OK                  57         57     0.475     0.467
  query19  OK/OK                   1          1     0.442     0.370
  query20  OK/OK                 186        186     0.546     0.677
  query21  OK/OK                 100        100     0.507     0.625
  query22  OK/OK                   7          7     0.500     0.398

## rapids
  query    status A/B         rows A     rows B    secs A    secs B
  query1   OK/OK                   4          4     5.374     2.536
  query2   OK/OK                 100        100     4.102     4.233
  query3   OK/OK                  10         10     5.339     3.575
  query4   OK/OK                   5          5     3.650     2.881
  query5   OK/OK                   5          5     4.647     4.460
  query6   OK/OK                   1          1     1.667     1.689
  query7   OK/OK                   4          4     5.684     5.095
  query8   OK/OK                   2          2     4.960     5.945
  query9   OK/OK                 175        175     5.741     5.141
  query10  OK/OK                  20         20     6.927     6.870
  query11  OK/OK               29531      29531     3.905     3.506
  query12  OK/OK                   2          2     2.966     2.596
  query13  OK/OK                  42         42     2.678     2.758
  query14  OK/OK                   1          1     2.706     2.664
  query15  OK/OK                   1          1     4.269     4.808
  query16  OK/OK               18314      18314     3.445     3.482
  query17  OK/OK                   1          1     3.668     4.958
  query18  OK/OK                  57         57     7.242     7.090
  query19  OK/OK                   1          1     4.630     4.170
  query20  OK/OK                 186        186     3.794     3.958
  query21  OK/OK                 100        100     7.186     7.573
  query22  OK/OK                   7          7     2.823     3.970

## sirius
  query    status A/B         rows A     rows B    secs A    secs B
  query1   OK/OK                   4          4     0.255     0.195
  query2   OK/OK                 100        100     1.229     0.337
  query3   OK/OK                  10         10     0.671     0.706
  query4   OK/OK                   5          5     0.298     0.314
  query5   OK/OK                   5          5     0.451     0.419
  query6   OK/OK                   1          1     0.235     0.245
  query7   OK/OK                   4          4     0.580     0.557
  query8   OK/OK                   2          2     0.760     0.757
  query9   OK/OK                 175        175     0.367     0.390
  query10  OK/OK                  20         20     0.565     0.550
  query11  OK/OK               29531      29531     0.249     0.299
  query12  OK/OK                   2          2     0.282     0.291
  query13  OK/OK                  42         42     0.247     0.238
  query14  OK/OK                   1          1     0.280     0.282
  query15  OK/OK                   1          1     0.312     0.300
  query16  OK/OK               18314      18314     0.300     0.303
  query17  OK/OK                   1          1     0.584     0.691
  query18  OK/OK                  57         57     0.382     0.429
  query19  OK/OK                   1          1     1.088     1.064
  query20  OK/OK                 186        186     0.548     0.628
  query21  OK/OK                 100        100     0.727     0.696
  query22  OK/OK                   7          7     0.262     0.284

## Cross-engine row counts (per dataset, OK queries only)

### upstream
  query     duckdb_cpu  polars_gpu      rapids      sirius
  query1             4           4           4           4
  query2           100         100         100         100
  query3            10          10          10          10
  query4             5           5           5           5
  query5             5           5           5           5
  query6             1           1           1           1
  query7             4           4           4           4
  query8             2           2           2           2
  query9           175         175         175         175
  query10           20          20          20          20
  query11        29531       29531       29531       29531
  query12            2           2           2           2
  query13           42          42          42          42
  query14            1           1           1           1
  query15            1           1           1           1
  query16        18314       18314       18314       18314
  query17            1           1           1           1
  query18           57          57          57          57
  query19            1           1           1           1
  query20          186         186         186         186
  query21          100         100         100         100
  query22            7           7           7           7

### datagen
  query     duckdb_cpu  polars_gpu      rapids      sirius
  query1             4           4           4           4
  query2           100         100         100         100
  query3            10          10          10          10
  query4             5           5           5           5
  query5             5           5           5           5
  query6             1           1           1           1
  query7             4           4           4           4
  query8             2           2           2           2
  query9           175         175         175         175
  query10           20          20          20          20
  query11        29531       29531       29531       29531
  query12            2           2           2           2
  query13           42          42          42          42
  query14            1           1           1           1
  query15            1           1           1           1
  query16        18314       18314       18314       18314
  query17            1           1           1           1
  query18           57          57          57          57
  query19            1           1           1           1
  query20          186         186         186         186
  query21          100         100         100         100
  query22            7           7           7           7

## Verdict
EQUIVALENT: every engine returns identical status and row counts on upstream and datagen, and all engines agree with each other.
