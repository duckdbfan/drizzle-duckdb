<div align="center">

# Drizzle DuckDB

### DuckDB dialect for [Drizzle ORM](https://orm.drizzle.team/)

[![npm version](https://img.shields.io/npm/v/@leonardovida-md/drizzle-neo-duckdb)](https://www.npmjs.com/package/@leonardovida-md/drizzle-neo-duckdb)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[Documentation](https://leonardovida.github.io/drizzle-neo-duckdb/) • [LLM Context](https://leonardovida.github.io/drizzle-neo-duckdb/llms.txt) • [Examples](./example) • [Contributing](#contributing)

</div>

<br>

**Drizzle DuckDB** brings [Drizzle ORM](https://orm.drizzle.team/) to [DuckDB](https://duckdb.org/) — the fast in-process analytical database. Get Drizzle's type-safe query builder, automatic migrations, and full TypeScript inference while working with DuckDB's powerful analytics engine.

Works with local DuckDB files, in-memory databases, and [MotherDuck](https://motherduck.com/) cloud.

> **Status:** Experimental. Core query building, migrations, and type inference work well. Some DuckDB-specific types and edge cases are still being refined.

Docs tip: every docs page has a **Markdown (raw)** button for LLM-friendly source.

## Installation

```bash
bun add @leonardovida-md/drizzle-neo-duckdb @duckdb/node-api
```

```bash
npm install @leonardovida-md/drizzle-neo-duckdb @duckdb/node-api
```

```bash
pnpm add @leonardovida-md/drizzle-neo-duckdb @duckdb/node-api
```

## Quick Start

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';
import { integer, text, pgTable } from 'drizzle-orm/pg-core';

// Connect to DuckDB
const instance = await DuckDBInstance.create(':memory:');
const connection = await instance.connect();
const db = drizzle(connection);

// Define your schema
const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  email: text('email').notNull(),
});

// Create table
await db.execute(sql`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
  )
`);

// Insert data
await db.insert(users).values([
  { id: 1, name: 'Alice', email: 'alice@example.com' },
  { id: 2, name: 'Bob', email: 'bob@example.com' },
]);

// Query with full type safety
const allUsers = await db.select().from(users);
//    ^? { id: number; name: string; email: string }[]

// Clean up
connection.closeSync();
```

## Connecting to DuckDB

### In-Memory Database

```typescript
const instance = await DuckDBInstance.create(':memory:');
const connection = await instance.connect();
const db = drizzle(connection);
```

### Local File

```typescript
const instance = await DuckDBInstance.create('./my-database.duckdb');
const connection = await instance.connect();
const db = drizzle(connection);
```

### MotherDuck Cloud

```typescript
const instance = await DuckDBInstance.create('md:', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
const connection = await instance.connect();
const db = drizzle(connection);
```

### With Logging

```typescript
import { DefaultLogger } from 'drizzle-orm';

const db = drizzle(connection, {
  logger: new DefaultLogger(),
});
```

## Schema Declaration

Drizzle DuckDB uses `drizzle-orm/pg-core` for schema definitions since DuckDB's SQL is largely Postgres-compatible:

```typescript
import { sql } from 'drizzle-orm';
import {
  integer,
  text,
  boolean,
  timestamp,
  pgTable,
  pgSchema,
} from 'drizzle-orm/pg-core';

// Tables in default schema
const posts = pgTable('posts', {
  id: integer('id').primaryKey(),
  title: text('title').notNull(),
  published: boolean('published').default(false),
  createdAt: timestamp('created_at').defaultNow(),
});

// Tables in custom schema
const analytics = pgSchema('analytics');

const events = analytics.table('events', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  timestamp: timestamp('timestamp').defaultNow(),
});
```

## DuckDB-Specific Column Types

For DuckDB-specific types like `STRUCT`, `MAP`, `LIST`, and proper timestamp handling, use the custom column helpers:

```typescript
import {
  duckDbList,
  duckDbArray,
  duckDbStruct,
  duckDbMap,
  duckDbJson,
  duckDbTimestamp,
  duckDbDate,
  duckDbTime,
} from '@leonardovida-md/drizzle-neo-duckdb';

const products = pgTable('products', {
  id: integer('id').primaryKey(),

  // LIST type (variable length)
  tags: duckDbList('tags', 'TEXT'),

  // ARRAY type (fixed length)
  coordinates: duckDbArray('coordinates', 'DOUBLE', 3),

  // STRUCT type
  metadata: duckDbStruct('metadata', {
    version: 'INTEGER',
    author: 'TEXT',
  }),

  // MAP type
  attributes: duckDbMap('attributes', 'TEXT'),

  // JSON type (use this instead of pg json/jsonb)
  config: duckDbJson('config'),

  // Timestamp with proper DuckDB handling
  createdAt: duckDbTimestamp('created_at', { withTimezone: true }),
});
```

See [Column Types Documentation](https://leonardovida.github.io/drizzle-neo-duckdb/api/columns) for complete reference.

## Querying

All standard Drizzle query methods work:

```typescript
// Select
const users = await db
  .select()
  .from(usersTable)
  .where(eq(usersTable.active, true));

// Insert
await db
  .insert(usersTable)
  .values({ name: 'Alice', email: 'alice@example.com' });

// Insert with returning
const inserted = await db
  .insert(usersTable)
  .values({ name: 'Bob' })
  .returning({ id: usersTable.id });

// Update
await db
  .update(usersTable)
  .set({ name: 'Updated' })
  .where(eq(usersTable.id, 1));

// Delete
await db.delete(usersTable).where(eq(usersTable.id, 1));
```

### Array Operations

For DuckDB array operations, use the custom helpers instead of Postgres operators:

```typescript
import {
  duckDbArrayContains,
  duckDbArrayContained,
  duckDbArrayOverlaps,
} from '@leonardovida-md/drizzle-neo-duckdb';

// Check if array contains all values
const results = await db
  .select()
  .from(products)
  .where(duckDbArrayContains(products.tags, ['electronics', 'sale']));

// Check if array is contained by values
const results = await db
  .select()
  .from(products)
  .where(
    duckDbArrayContained(products.tags, ['electronics', 'sale', 'featured'])
  );

// Check if arrays overlap
const results = await db
  .select()
  .from(products)
  .where(duckDbArrayOverlaps(products.tags, ['electronics', 'books']));
```

## Transactions

```typescript
await db.transaction(async (tx) => {
  await tx.insert(accounts).values({ balance: 100 });
  await tx.update(accounts).set({ balance: 50 }).where(eq(accounts.id, 1));
});
```

> **Note:** DuckDB doesn't support `SAVEPOINT`, so nested transactions reuse the outer transaction context. Inner rollbacks will abort the entire transaction.

## Migrations

Apply SQL migration files using the `migrate` function:

```typescript
import { migrate } from '@leonardovida-md/drizzle-neo-duckdb';

await migrate(db, { migrationsFolder: './drizzle' });
```

Migration metadata is stored in `drizzle.__drizzle_migrations` by default. See [Migrations Documentation](https://leonardovida.github.io/drizzle-neo-duckdb/guide/migrations) for configuration options.

## Schema Introspection

Generate Drizzle schema from an existing DuckDB database:

### CLI

```bash
bunx duckdb-introspect --url ./my-database.duckdb --out ./drizzle/schema.ts
```

### Programmatic

```typescript
import { introspect } from '@leonardovida-md/drizzle-neo-duckdb';

const result = await introspect(db, {
  schemas: ['public', 'analytics'],
  includeViews: true,
});

console.log(result.files.schemaTs);
```

See [Introspection Documentation](https://leonardovida.github.io/drizzle-neo-duckdb/guide/introspection) for all options.

## Configuration Options

```typescript
const db = drizzle(connection, {
  // Enable query logging
  logger: new DefaultLogger(),

  // Rewrite Postgres array operators to DuckDB functions (default: true)
  rewriteArrays: true,

  // Throw on Postgres-style array literals like '{1,2,3}' (default: false)
  rejectStringArrayLiterals: false,

  // Pass your schema for relational queries
  schema: mySchema,
});
```

## Known Limitations

This connector aims for compatibility with Drizzle's Postgres driver but has some differences:

| Feature               | Status                                                     |
| --------------------- | ---------------------------------------------------------- |
| Basic CRUD operations | Full support                                               |
| Joins and subqueries  | Full support                                               |
| Transactions          | No savepoints (nested transactions reuse outer)            |
| JSON/JSONB columns    | Use `duckDbJson()` instead                                 |
| Prepared statements   | No statement caching                                       |
| Streaming results     | Materialized by default; use `executeBatches()` for chunks |

See [Limitations Documentation](https://leonardovida.github.io/drizzle-neo-duckdb/guide/limitations) for details.

## Examples

- **[MotherDuck NYC Taxi](./example/motherduck-nyc.ts)** — Query the built-in NYC taxi dataset from MotherDuck cloud

Run examples:

```bash
MOTHERDUCK_TOKEN=your_token bun example/motherduck-nyc.ts
```

## Contributing

Contributions are welcome! Please:

1. Include tests for new features (`test/<feature>.test.ts`)
2. Note any DuckDB-specific quirks you encounter
3. Use a clear, imperative commit message

```bash
# Install dependencies
bun install

# Run tests
bun test

# Run tests with UI
bun t

# Build
bun run build
```

## License

[Apache-2.0](./LICENSE)
