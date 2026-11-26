# duckdb.gsa

Native DuckDB 2.x bindings and composable Query DSL for Geisha.

## Modules

- `duckdb.duckdb` — `Database`, `Connection`, `QueryResult`, `PreparedStmt`, chunk-based vectorized column extraction (`resColToFloatArray`, `resColToInt64Array`, `resColToStrArray`, `resColCopyF64Ptr`), Parquet/CSV helpers
- `duckdb.dsl` — Composable pipeline Query DSL (`|>`) so queries never need raw SQL strings:

```geisha
fromParquet(path)
  |> filter(and_(isNotNull(col("close")), gt(col("close"), litFloat(1000.0))))
  |> select([col("sym"), asCol(avg(col("close")), "avg_close"), asCol(countAll(), "n")])
  |> groupBy(["sym"])
  |> orderByDesc("avg_close")
  |> limit(10)
```

## Test

```sh
geisha test
```
