---
layout: default
title: Database Connection
parent: Core Concepts
nav_order: 1
---

# Database Connection

Learn how to connect to DuckDB databases in different scenarios.

## In-Memory Database

Perfect for testing and temporary data processing:

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';

const instance = await DuckDBInstance.create(':memory:');
const connection = await instance.connect();
const db = drizzle(connection);
```

Data is lost when the connection closes.

## Local File

Persist your data to disk:

```typescript
const instance = await DuckDBInstance.create('./my-database.duckdb');
const connection = await instance.connect();
const db = drizzle(connection);
```

The file is created if it doesn't exist.

## MotherDuck Cloud

Connect to [MotherDuck](https://motherduck.com/) for cloud-hosted DuckDB:

```typescript
const instance = await DuckDBInstance.create('md:', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
const connection = await instance.connect();
const db = drizzle(connection);
```

See the [MotherDuck guide](/integrations/motherduck) for more details.

## With Logging

Enable query logging for debugging:

```typescript
import { DefaultLogger } from 'drizzle-orm';

const db = drizzle(connection, {
  logger: new DefaultLogger(),
});
```

Or simply:

```typescript
const db = drizzle(connection, { logger: true });
```

## With Schema

Pass your schema for relational queries:

```typescript
import * as schema from './schema';

const db = drizzle(connection, { schema });

// Now relational queries work
const usersWithPosts = await db.query.users.findMany({
  with: { posts: true },
});
```

## Connection Patterns

### Singleton (Recommended for Long-Running Apps)

```typescript
// db.ts
import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import { drizzle, DuckDBDatabase } from '@leonardovida-md/drizzle-neo-duckdb';
import * as schema from './schema';

let instance: DuckDBInstance | null = null;
let connection: DuckDBConnection | null = null;

export async function getDb(): Promise<DuckDBDatabase<typeof schema>> {
  if (!instance) {
    instance = await DuckDBInstance.create('./app.duckdb');
  }
  if (!connection) {
    connection = await instance.connect();
  }
  return drizzle(connection, { schema });
}
```

### Cleanup Pattern (Serverless/Short-Lived)

```typescript
export async function withDb<T>(
  callback: (db: DuckDBDatabase) => Promise<T>
): Promise<T> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();

  try {
    const db = drizzle(connection);
    return await callback(db);
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
}

// Usage
const users = await withDb(async (db) => {
  return db.select().from(usersTable);
});
```

### Multiple Connections

DuckDB supports multiple connections to the same database:

```typescript
const instance = await DuckDBInstance.create('./app.duckdb');

// Each connection can execute queries independently
const conn1 = await instance.connect();
const conn2 = await instance.connect();

const db1 = drizzle(conn1);
const db2 = drizzle(conn2);
```

## Configuration Options

```typescript
const db = drizzle(connection, {
  // Enable query logging
  logger: true,

  // Or use custom logger
  logger: new DefaultLogger(),

  // Schema for relational queries
  schema: mySchema,

  // Rewrite Postgres array operators to DuckDB (default: true)
  rewriteArrays: true,

  // Throw on Postgres-style array literals (default: false)
  rejectStringArrayLiterals: false,
});
```

See [Configuration](/reference/configuration) for all options.

## Closing Connections

Always clean up connections when done:

```typescript
const instance = await DuckDBInstance.create('./app.duckdb');
const connection = await instance.connect();
const db = drizzle(connection);

try {
  // Use db...
} finally {
  connection.closeSync();
  instance.closeSync();
}
```

## Error Handling

```typescript
try {
  const instance = await DuckDBInstance.create('./database.duckdb');
  const connection = await instance.connect();
  const db = drizzle(connection);

  // Use database...
} catch (error) {
  if (error.message.includes('Permission denied')) {
    console.error('Cannot write to database file');
  } else if (error.message.includes('Could not open')) {
    console.error('Database file not found or corrupted');
  } else {
    throw error;
  }
}
```

## See Also

- [drizzle()]({{ '/api/drizzle' | relative_url }}) - API reference
- [Configuration]({{ '/reference/configuration' | relative_url }}) - All options
- [MotherDuck]({{ '/integrations/motherduck' | relative_url }}) - Cloud connection
