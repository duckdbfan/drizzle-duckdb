# Using with Next.js

This guide covers how to use `@leonardovida-md/drizzle-neo-duckdb` with Next.js applications.

## Configuration

Since `@duckdb/node-api` is a native Node.js module, Next.js requires explicit configuration to handle it correctly during builds.

### Next.js 15+

```javascript
// next.config.js
/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['@duckdb/node-api'],
};

module.exports = nextConfig;
```

### Next.js 14

```javascript
// next.config.js
/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    serverComponentsExternalPackages: ['@duckdb/node-api'],
  },
};

module.exports = nextConfig;
```

## Environment Variables

For MotherDuck connections, set your token as a server-side environment variable:

```bash
# .env.local
MOTHERDUCK_TOKEN=your_token_here
```

**Important**: Do NOT prefix with `NEXT_PUBLIC_` as this would expose your token to the client bundle.

## Creating a Database Client

Create a singleton database client to reuse connections:

```typescript
// lib/db.ts
import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import { drizzle, DuckDBDatabase } from '@leonardovida-md/drizzle-neo-duckdb';
import * as schema from './schema';

let instance: DuckDBInstance | null = null;
let connection: DuckDBConnection | null = null;

export async function getDb(): Promise<DuckDBDatabase<typeof schema>> {
  if (!instance) {
    const token = process.env.MOTHERDUCK_TOKEN;
    instance = token
      ? await DuckDBInstance.create('md:', { motherduck_token: token })
      : await DuckDBInstance.create(':memory:');
  }

  if (!connection) {
    connection = await instance.connect();
  }

  return drizzle(connection, { schema });
}
```

## Usage Examples

### API Routes (Route Handlers)

```typescript
// app/api/users/route.ts
import { getDb } from '@/lib/db';
import { users } from '@/lib/schema';
import { NextResponse } from 'next/server';

export async function GET() {
  const db = await getDb();
  const allUsers = await db.select().from(users);
  return NextResponse.json(allUsers);
}

export async function POST(request: Request) {
  const db = await getDb();
  const body = await request.json();

  const newUser = await db
    .insert(users)
    .values({ name: body.name, email: body.email })
    .returning();

  return NextResponse.json(newUser[0], { status: 201 });
}
```

### Server Components

```typescript
// app/dashboard/page.tsx
import { getDb } from '@/lib/db';
import { analytics } from '@/lib/schema';

export default async function DashboardPage() {
  const db = await getDb();
  const stats = await db.select().from(analytics).limit(10);

  return (
    <div>
      <h1>Dashboard</h1>
      <ul>
        {stats.map((stat) => (
          <li key={stat.id}>{stat.name}: {stat.value}</li>
        ))}
      </ul>
    </div>
  );
}
```

### Server Actions

```typescript
// app/actions.ts
'use server';

import { getDb } from '@/lib/db';
import { users } from '@/lib/schema';
import { revalidatePath } from 'next/cache';

export async function createUser(formData: FormData) {
  const db = await getDb();
  const name = formData.get('name') as string;
  const email = formData.get('email') as string;

  await db.insert(users).values({ name, email });
  revalidatePath('/users');
}
```

## Runtime Restrictions

### Edge Runtime: Not Supported

`@duckdb/node-api` is a native Node.js module and **cannot** run on the Edge Runtime. If you try to use it in an edge function, you'll see an error like:

```
Native Node.js APIs are not supported in Edge Runtime
```

Ensure your routes using DuckDB are configured for the Node.js runtime:

```typescript
// app/api/data/route.ts
export const runtime = 'nodejs'; // Explicitly use Node.js runtime
```

### Client Components: Not Supported

DuckDB can only be used server-side. Do not attempt to import or use the database client in client components (files with `'use client'`).

For client-side data, fetch from API routes:

```typescript
// components/UserList.tsx
'use client';

import { useEffect, useState } from 'react';

export function UserList() {
  const [users, setUsers] = useState([]);

  useEffect(() => {
    fetch('/api/users')
      .then((res) => res.json())
      .then(setUsers);
  }, []);

  return <ul>{users.map((u) => <li key={u.id}>{u.name}</li>)}</ul>;
}
```

## Troubleshooting

### "Module parse failed" Error

If you see webpack parsing errors for `@duckdb/node-api`:

```
Module parse failed: Unexpected character '...'
```

**Solution**: Add `serverExternalPackages` to your `next.config.js` (see Configuration section above).

### "Native Node.js APIs not supported" Error

This occurs when trying to use DuckDB in Edge Runtime.

**Solution**: Ensure your route uses the Node.js runtime by adding `export const runtime = 'nodejs'` or not specifying edge deployment.

### GLIBCXX Errors on Vercel

You may see errors related to GLIBCXX version compatibility on some Vercel deployment regions:

```
Error: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.26' not found
```

**Solution**: Try deploying to a different Vercel region, or use a Docker-based deployment that includes a compatible runtime.

### Connection Cleanup in Serverless

In serverless environments, connections may not be properly cleaned up between invocations. Consider implementing connection pooling or cleanup logic:

```typescript
// lib/db.ts
import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';

export async function withDb<T>(
  callback: (db: ReturnType<typeof drizzle>) => Promise<T>
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
```

## Complete Example

Here's a complete example of a Next.js app with DuckDB:

**`next.config.js`**:

```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['@duckdb/node-api'],
};
module.exports = nextConfig;
```

**`lib/schema.ts`**:

```typescript
import { pgTable, integer, text, timestamp } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  email: text('email').notNull(),
  createdAt: timestamp('created_at').defaultNow(),
});
```

**`lib/db.ts`**:

```typescript
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';
import * as schema from './schema';

let db: ReturnType<typeof drizzle<typeof schema>> | null = null;

export async function getDb() {
  if (!db) {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    db = drizzle(connection, { schema });
  }
  return db;
}
```

**`app/api/users/route.ts`**:

```typescript
import { getDb } from '@/lib/db';
import { users } from '@/lib/schema';
import { NextResponse } from 'next/server';

export async function GET() {
  const db = await getDb();
  const result = await db.select().from(users);
  return NextResponse.json(result);
}
```
