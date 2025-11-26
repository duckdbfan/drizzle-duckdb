---
layout: default
title: MotherDuck
parent: Integrations
nav_order: 2
---

# MotherDuck

[MotherDuck](https://motherduck.com/) is a cloud-hosted DuckDB service that lets you run analytical queries without managing infrastructure.

## Getting Started

### Create an Account

1. Sign up at [motherduck.com](https://motherduck.com/)
2. Generate an API token from your account settings
3. Store the token securely (never commit to version control)

### Connect

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';

const instance = await DuckDBInstance.create('md:', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
const connection = await instance.connect();
const db = drizzle(connection);
```

{: .highlight }

> **Environment Variables**
>
> Always use environment variables for tokens. Never hardcode credentials.

## Connecting to a Specific Database

By default, connecting to `md:` opens your default MotherDuck database. To connect to a specific database:

```typescript
const instance = await DuckDBInstance.create('md:my_database', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
```

Or use the database in your queries:

```typescript
import { sql } from 'drizzle-orm';

// Use a specific database
const results = await db.execute(sql`
  SELECT * FROM my_database.main.users
`);
```

## Querying Sample Data

MotherDuck provides sample datasets. Here's an example querying the NYC taxi dataset:

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';
import { sql } from 'drizzle-orm';

const instance = await DuckDBInstance.create('md:', {
  motherduck_token: process.env.MOTHERDUCK_TOKEN,
});
const connection = await instance.connect();
const db = drizzle(connection);

// Query the sample NYC taxi data
const trips = await db.execute(sql`
  SELECT
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount
  FROM sample_data.nyc.taxi
  WHERE pickup_datetime >= '2019-01-01'
  LIMIT 10
`);

console.log(trips);

connection.closeSync();
```

## Schema Introspection

When introspecting MotherDuck databases, be aware that:

1. **Default behavior**: Only your current database is introspected
2. **Shared databases**: Databases like `sample_data` won't be included unless explicitly requested

```bash
# Introspect your specific database
MOTHERDUCK_TOKEN=xxx bunx duckdb-introspect --url md: --database my_db --out ./schema.ts

# Introspect all databases (including shared)
MOTHERDUCK_TOKEN=xxx bunx duckdb-introspect --url md: --all-databases --out ./schema.ts
```

See [Introspection]({{ '/features/introspection' | relative_url }}) for more details.

## Hybrid Queries

MotherDuck supports hybrid queries that combine local and cloud data:

```typescript
import { sql } from 'drizzle-orm';

// Attach a local database
await db.execute(sql`ATTACH './local.duckdb' AS local_db`);

// Query across local and cloud
const results = await db.execute(sql`
  SELECT c.name, l.value
  FROM my_cloud_db.main.cloud_table c
  JOIN local_db.main.local_table l ON c.id = l.cloud_id
`);
```

## Performance Tips

### Use Appropriate Instance Sizes

MotherDuck offers different instance sizes. For large analytical queries, consider upgrading your instance.

### Leverage Caching

MotherDuck caches query results. Repeated queries on the same data will be faster.

### Batch Operations

For writes, batch your operations:

```typescript
// Better: single batch insert
await db.insert(events).values(manyEvents);

// Less efficient: individual inserts
for (const event of manyEvents) {
  await db.insert(events).values(event);
}
```

## Limitations

- **Write throughput**: MotherDuck is optimized for analytical reads. For high-frequency writes, consider local DuckDB.
- **Connection limits**: Be mindful of connection limits in serverless environments.
- **Cold starts**: First query after idle may be slower due to instance spin-up.

## Example: Analytics Dashboard

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';
import { sql } from 'drizzle-orm';

async function getAnalytics() {
  const instance = await DuckDBInstance.create('md:', {
    motherduck_token: process.env.MOTHERDUCK_TOKEN,
  });
  const connection = await instance.connect();
  const db = drizzle(connection);

  try {
    // Get daily aggregates
    const dailyStats = await db.execute(sql`
      SELECT
        DATE_TRUNC('day', created_at) as date,
        COUNT(*) as count,
        SUM(revenue) as total_revenue
      FROM my_database.main.transactions
      WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
      GROUP BY 1
      ORDER BY 1
    `);

    return dailyStats;
  } finally {
    connection.closeSync();
  }
}
```

## Resources

- [MotherDuck Documentation](https://motherduck.com/docs/)
- [MotherDuck Sample Datasets](https://motherduck.com/docs/getting-started/sample-data/)
- [DuckDB MotherDuck Extension](https://duckdb.org/docs/extensions/motherduck.html)
