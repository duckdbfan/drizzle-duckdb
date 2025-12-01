# Architecture Map

## Entry Points and Runtime Modes

- `src/driver.ts`: `drizzle(...)` factory (5 overloads) chooses single connection (`DuckDBInstance.create(path).connect()`) or pooled (`createDuckDBConnectionPool(instance, { size })`). Builds `DuckDBDatabase` which exposes `executeBatches`, `executeBatchesRaw`, `executeArrow`, `transaction`, and surface `$client` plus `$instance` for visibility.
- `src/pool.ts`: FIFO pool that wraps `DuckDBConnection.create(instance)` with acquire/release and recycling via `maxLifetimeMs`/`idleTimeoutMs`.

## Drizzle Integration Points

- `src/dialect.ts`: `DuckDBDialect` extends `PgDialect`, overrides `prepareTyping()` and `migrate()`, rejects `PgJson/PgJsonb`, caches savepoint support per instance.
- `src/session.ts`: `DuckDBSession` extends `PgSession`; rewrites array operators when enabled; resets JSON flag per query; pins pooled connections for transactions; nested transactions use savepoints with capability probe. Adds streaming (`executeBatches`, `executeBatchesRaw`), Arrow fetch, and prepared query wiring.
- `DuckDBPreparedQuery.execute()`: builds params, optional array rewrite, uses client helpers (`executeArraysOnClient` for projections, `executeOnClient` otherwise), then maps rows through `mapResultRow` or custom mapper.

## Client and Value Conversion

- `src/client.ts`: parameter prep (`prepareParams`), value conversion (`toNodeApiValue`), materialized execution (`executeOnClient`, `executeArraysOnClient`), streaming (`executeInBatches`, `executeInBatchesRaw`), Arrow path, prepared statement cache (optional, per connection), column name deduplication guard, and connection cleanup.
- `src/value-wrappers*.ts`: wrappers for list/array/struct/map/json/blob/timestamp with fast conversion to DuckDB Node API values.

## DuckDB Types, Helpers, and Rewriting

- `src/columns.ts`: DuckDB-specific column helpers (`duckDbList/Array/Map/Struct/Json/Blob/Timestamp` etc.), literal builders, array helper predicates (`duckDbArrayContains/Contained/Overlaps`). Timestamp binding defaults to parameter binding on Node with literal fallback for Bun or explicit override.
- `src/sql/query-rewriters.ts`: `adaptArrayOperators()` rewrites `@>`, `<@`, `&&` to DuckDB `array_has_*` functions with a fast-path guard and comment/string scrubbing.
- `src/sql/result-mapper.ts`: normalizes inet/time/timestamp/date/interval and maps nested selection objects with nullable join nullification.

## Introspection and CLI

- `src/introspect.ts` and `src/bin/duckdb-introspect.ts`: read `information_schema` and `duckdb_*` tables to emit `schema.ts` plus JSON metadata for tooling.

## Examples and Perf Harness

- `examples/`, `test/perf/`, `scripts/run-perf.ts`, `scripts/compare-perf.ts`: tinybench-based micro-benchmarks covering builder paths, streaming (object and raw array), Arrow, prepared reuse, and pooled mode. Perf scripts emit JSON with ops/s, latency percentiles, and memory snapshots for regression checks.
