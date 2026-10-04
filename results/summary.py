#!/usr/bin/env python3
"""Print the standard pivots of results/all_results.csv (`make summary`):
per (engine, SF) the OK count and total OK seconds, then the query x engine_sf
seconds matrix. Both are views of the one results table, never saved."""
import os
import sys

import duckdb

csv = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                           "all_results.csv")
con = duckdb.connect()
con.execute(f"CREATE VIEW r AS SELECT * FROM read_csv('{csv}', all_varchar=true)")
con.sql("""SELECT engine, scale_factor::INT AS sf, count(*) FILTER (status = 'OK') AS ok,
                  count(*) AS queries,
                  round(sum(TRY_CAST(seconds AS DOUBLE)) FILTER (status = 'OK'), 1) AS total_ok_s
           FROM r GROUP BY ALL ORDER BY sf, engine""").show(max_rows=1000)
con.sql("""PIVOT (SELECT query, engine || '_sf' || scale_factor AS run,
                         CASE WHEN status = 'OK' THEN seconds ELSE status END AS v FROM r)
           ON run USING first(v) GROUP BY query
           ORDER BY TRY_CAST(replace(query, 'query', '') AS INT)""").show(max_rows=1000, max_width=10000)
