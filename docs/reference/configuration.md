---
layout: default
title: Configuration
parent: Reference
nav_order: 1
---

# Configuration

Complete reference for all configuration options in Drizzle DuckDB.

## drizzle() Options

The `drizzle()` function accepts a configuration object:

```typescript
const db = drizzle(connection, {
  logger: true,
  schema: mySchema,
  rewriteArrays: true,
  rejectStringArrayLiterals: false,
});
```

### logger

Enable query logging for debugging.

| Type                | Default     | Description                             |
| ------------------- | ----------- | --------------------------------------- |
| `boolean \| Logger` | `undefined` | Enable logging or provide custom logger |

**Usage**:

```typescript
// Use default logger (logs to console)
const db = drizzle(connection, { logger: true });

// Use custom logger
import { DefaultLogger } from 'drizzle-orm';

const db = drizzle(connection, {
  logger: new DefaultLogger({
    writer: {
      write(message: string) {
        // Custom logging logic
        myLogger.debug(message);
      },
    },
  }),
});
```

**Example output**:

```
Query: SELECT * FROM users WHERE id = $1
Params: [1]
```

### schema

Schema definition for relational queries.

| Type                      | Default     | Description                             |
| ------------------------- | ----------- | --------------------------------------- |
| `Record<string, unknown>` | `undefined` | Schema object with tables and relations |

**Usage**:

```typescript
// schema.ts
import { pgTable, integer, text } from 'drizzle-orm/pg-core';
import { relations } from 'drizzle-orm';

export const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
});

export const posts = pgTable('posts', {
  id: integer('id').primaryKey(),
  userId: integer('user_id'),
  title: text('title'),
});

export const usersRelations = relations(users, ({ many }) => ({
  posts: many(posts),
}));

// db.ts
import * as schema from './schema';

const db = drizzle(connection, { schema });

// Now relational queries work
const usersWithPosts = await db.query.users.findMany({
  with: { posts: true },
});
```

### rewriteArrays

Automatically rewrite Postgres array operators to DuckDB functions.

| Type      | Default | Description                         |
| --------- | ------- | ----------------------------------- |
| `boolean` | `true`  | Enable automatic operator rewriting |

**Behavior when enabled**:

| Postgres Operator   | Rewritten To                 |
| ------------------- | ---------------------------- |
| `@>` (contains)     | `array_has_all(left, right)` |
| `<@` (contained by) | `array_has_all(right, left)` |
| `&&` (overlaps)     | `array_has_any(left, right)` |

**Usage**:

```typescript
// Default: rewriting enabled
const db = drizzle(connection, { rewriteArrays: true });

// Postgres-style code works automatically
const results = await db
  .select()
  .from(products)
  .where(arrayContains(products.tags, ['sale']));
// Generated: WHERE array_has_all(tags, ['sale'])

// Disable rewriting (use DuckDB syntax directly)
const db = drizzle(connection, { rewriteArrays: false });
```

### rejectStringArrayLiterals

Throw an error when Postgres-style array literals are detected.

| Type      | Default | Description                                    |
| --------- | ------- | ---------------------------------------------- |
| `boolean` | `false` | Throw instead of warning on `'{...}'` literals |

**Usage**:

```typescript
// Default: logs warning
const db = drizzle(connection, { rejectStringArrayLiterals: false });
await db.execute(sql`SELECT * FROM t WHERE tags = '{a,b}'`);
// Warning logged, query may fail

// Strict mode: throws error
const db = drizzle(connection, { rejectStringArrayLiterals: true });
await db.execute(sql`SELECT * FROM t WHERE tags = '{a,b}'`);
// Error: Postgres-style array literals are not supported
```

## migrate() Options

The `migrate()` function accepts either a string path or configuration object:

```typescript
// Simple: just the path
await migrate(db, './drizzle');

// Full configuration
await migrate(db, {
  migrationsFolder: './drizzle',
  migrationsTable: '__drizzle_migrations',
  migrationsSchema: 'drizzle',
});
```

### migrationsFolder

Path to the folder containing SQL migration files.

| Type     | Default  | Description                  |
| -------- | -------- | ---------------------------- |
| `string` | Required | Path to migrations directory |

### migrationsTable

Name of the table used to track applied migrations.

| Type     | Default                  | Description                   |
| -------- | ------------------------ | ----------------------------- |
| `string` | `'__drizzle_migrations'` | Migration tracking table name |

### migrationsSchema

Schema where the migrations tracking table is created.

| Type     | Default     | Description                 |
| -------- | ----------- | --------------------------- |
| `string` | `'drizzle'` | Schema for migrations table |

## introspect() Options

Options for schema introspection:

```typescript
const result = await introspect(db, {
  database: 'my_database',
  schemas: ['main', 'analytics'],
  includeViews: true,
  useCustomTimeTypes: true,
  mapJsonAsDuckDbJson: true,
  importBasePath: '@leonardovida-md/drizzle-neo-duckdb/helpers',
});
```

### database

Specific database to introspect.

| Type     | Default          | Description          |
| -------- | ---------------- | -------------------- |
| `string` | Current database | Target database name |

### allDatabases

Introspect all attached databases (ignored if `database` is set).

| Type      | Default | Description                    |
| --------- | ------- | ------------------------------ |
| `boolean` | `false` | Include all attached databases |

### schemas

Specific schemas to introspect.

| Type       | Default                | Description        |
| ---------- | ---------------------- | ------------------ |
| `string[]` | All non-system schemas | Schemas to include |

### includeViews

Include views in the output.

| Type      | Default | Description               |
| --------- | ------- | ------------------------- |
| `boolean` | `false` | Generate schema for views |

### useCustomTimeTypes

Use DuckDB-specific timestamp types.

| Type      | Default | Description                |
| --------- | ------- | -------------------------- |
| `boolean` | `true`  | Use `duckDbTimestamp` etc. |

When `true`, generates:

```typescript
createdAt: duckDbTimestamp('created_at'),
```

When `false`, generates:

```typescript
createdAt: timestamp('created_at'),
```

### mapJsonAsDuckDbJson

Map JSON columns to `duckDbJson`.

| Type      | Default | Description                       |
| --------- | ------- | --------------------------------- |
| `boolean` | `true`  | Use `duckDbJson` for JSON columns |

### importBasePath

Base path for local type imports.

| Type     | Default                                         | Description           |
| -------- | ----------------------------------------------- | --------------------- |
| `string` | `'@leonardovida-md/drizzle-neo-duckdb/helpers'` | Import path for types |

## Environment Variables

Common environment variables used with Drizzle DuckDB:

### MOTHERDUCK_TOKEN

Authentication token for MotherDuck connections.

```bash
MOTHERDUCK_TOKEN=your_token_here
```

```typescript
const instance = await DuckDBInstance.create('md:', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
```

## TypeScript Configuration

Recommended `tsconfig.json` settings:

```json
{
  "compilerOptions": {
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true
  }
}
```

## See Also

- [drizzle()]({{ '/api/drizzle' | relative_url }}) - API reference
- [migrate()]({{ '/api/migrate' | relative_url }}) - Migration API
- [introspect()]({{ '/api/introspect' | relative_url }}) - Introspection API
