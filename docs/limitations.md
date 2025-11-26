# Limitations and Differences

This page documents known differences between Drizzle DuckDB and Drizzle's standard Postgres driver.

## Feature Support Matrix

| Feature              | Status  | Notes                               |
| -------------------- | ------- | ----------------------------------- |
| Select queries       | Full    | All standard select operations work |
| Insert/Update/Delete | Full    | Including `.returning()`            |
| Joins                | Full    | All join types supported            |
| Subqueries           | Full    |                                     |
| CTEs (WITH clauses)  | Full    |                                     |
| Aggregations         | Full    |                                     |
| Transactions         | Partial | No savepoints                       |
| Prepared statements  | Partial | No statement caching                |
| JSON/JSONB columns   | None    | Use `duckDbJson()` instead          |
| Streaming results    | None    | Results are materialized            |
| Relational queries   | Full    | With schema configuration           |

## Transactions

### No Savepoint Support

DuckDB doesn't support `SAVEPOINT`, which means nested transactions behave differently:

```typescript
// In Postgres: inner rollback only affects inner transaction
// In DuckDB: inner rollback aborts the ENTIRE transaction

await db.transaction(async (tx) => {
  await tx.insert(users).values({ id: 1, name: 'Alice' });

  await tx.transaction(async (innerTx) => {
    await innerTx.insert(users).values({ id: 2, name: 'Bob' });
    // This rollback aborts EVERYTHING, including Alice
    innerTx.rollback();
  });
});
```

**Workaround:** Structure your code to avoid nested transactions, or handle rollback logic at the outer level.

## JSON Columns

### Postgres JSON/JSONB Not Supported

Using `json()` or `jsonb()` from `drizzle-orm/pg-core` will throw an error:

```typescript
import { json, jsonb } from 'drizzle-orm/pg-core';

// This will throw at runtime
const table = pgTable('example', {
  data: json('data'), // Error!
});
```

**Solution:** Use `duckDbJson()` instead:

```typescript
import { duckDbJson } from '@leonardovida-md/drizzle-neo-duckdb';

const table = pgTable('example', {
  data: duckDbJson('data'), // Works!
});
```

The driver checks for Postgres JSON columns and throws a descriptive error if found.

## Prepared Statements

### No Statement Caching

Unlike the Postgres driver, DuckDB doesn't cache prepared statements. Each query is prepared and executed fresh:

```typescript
// These execute as separate preparations
const result1 = await db.select().from(users).where(eq(users.id, 1));
const result2 = await db.select().from(users).where(eq(users.id, 2));
```

This has minimal performance impact for most workloads since DuckDB is optimized for analytical queries.

## Result Handling

### Materialized Results

All query results are fully materialized in memory. There's no cursor-based streaming:

```typescript
// This loads ALL matching rows into memory
const allUsers = await db.select().from(users);
```

**For large datasets:** Use `LIMIT` and pagination, or leverage DuckDB's native capabilities for large-scale analysis.

### Column Alias Deduplication

When selecting the same column multiple times (e.g., in complex joins), duplicate aliases are automatically suffixed to avoid collisions:

```typescript
const result = await db
  .select({
    userId: users.id,
    postId: posts.id, // Would conflict without deduplication
  })
  .from(users)
  .innerJoin(posts, eq(users.id, posts.userId));

// Columns are properly distinguished in results
```

## Date/Time Handling

### DuckDB Timestamp Semantics

DuckDB handles timestamps slightly differently than Postgres:

1. **No implicit timezone conversion** — Timestamps without timezone are stored as-is
2. **String format** — DuckDB uses space separator (`2024-01-15 10:30:00`) rather than `T`
3. **Offset normalization** — Timezone offsets like `+00` are handled correctly

The `duckDbTimestamp()` helper normalizes these differences:

```typescript
// Input: JavaScript Date or ISO string
await db.insert(events).values({
  createdAt: new Date('2024-01-15T10:30:00Z'),
});

// Output: Properly formatted for DuckDB queries
// SELECT ... WHERE created_at = TIMESTAMP '2024-01-15 10:30:00+00'
```

### Mode Options

```typescript
// Return Date objects (default)
duckDbTimestamp('col', { mode: 'date' });

// Return strings in DuckDB format
duckDbTimestamp('col', { mode: 'string' });
// Returns: '2024-01-15 10:30:00+00'
```

## Array Operators

### Postgres Operators Rewritten

By default, Postgres array operators are rewritten to DuckDB functions:

| Postgres               | DuckDB Equivalent              |
| ---------------------- | ------------------------------ |
| `column @> ARRAY[...]` | `array_has_all(column, [...])` |
| `column <@ ARRAY[...]` | `array_has_all([...], column)` |
| `column && ARRAY[...]` | `array_has_any(column, [...])` |

This happens automatically with `rewriteArrays: true` (default).

**Recommendation:** Use the explicit helpers for clarity:

```typescript
import { duckDbArrayContains } from '@leonardovida-md/drizzle-neo-duckdb';

// Explicit and clear
.where(duckDbArrayContains(products.tags, ['a', 'b']))

// Also works (auto-rewritten), but less clear
.where(arrayContains(products.tags, ['a', 'b']))
```

### String Array Literals

Postgres-style array literals like `'{1,2,3}'` are detected and logged as warnings:

```typescript
// This triggers a warning
await db.execute(sql`SELECT * FROM t WHERE tags = '{a,b,c}'`);
// Warning: Use duckDbList()/duckDbArray() or pass native arrays instead
```

To throw instead of warn:

```typescript
const db = drizzle(connection, {
  rejectStringArrayLiterals: true, // Throws on '{...}' literals
});
```

## Schema Features

### Sequences

DuckDB supports sequences, but with some differences:

- Sequences are schema-scoped
- The migration system creates sequences for tracking tables automatically
- `nextval()` and `currval()` work as expected

### Schemas

Custom schemas work, but DuckDB's default schema is `main` (not `public` like Postgres):

```typescript
// Works
const mySchema = pgSchema('analytics');
const table = mySchema.table('events', { ... });

// Default schema in DuckDB is 'main', not 'public'
```

## Performance Considerations

### Analytical vs OLTP

DuckDB is optimized for analytical workloads (OLAP), not transactional workloads (OLTP):

- **Good for:** Aggregations, scans, complex joins on large datasets
- **Less optimal for:** High-frequency single-row inserts/updates

For write-heavy workloads, consider batching:

```typescript
// Better: batch inserts
await db.insert(events).values(manyEvents);

// Less efficient: individual inserts in a loop
for (const event of manyEvents) {
  await db.insert(events).values(event); // Many round trips
}
```

### Memory Usage

Results are materialized in memory. For very large result sets:

```typescript
// Add LIMIT for large tables
const page = await db.select().from(hugeTable).limit(1000).offset(0);
```

## Workarounds Summary

| Limitation               | Workaround                                  |
| ------------------------ | ------------------------------------------- |
| No savepoints            | Avoid nested transactions                   |
| No JSON/JSONB            | Use `duckDbJson()`                          |
| No streaming             | Use pagination with LIMIT/OFFSET            |
| String array warnings    | Use native arrays or DuckDB helpers         |
| Default schema is `main` | Explicitly use `pgSchema('main')` if needed |
