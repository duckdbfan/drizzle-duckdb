import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { beforeAll, beforeEach, afterAll, expect, test } from 'vitest';
import { drizzle } from '../src';
import type { DuckDBDatabase } from '../src';

const nodeUsers = pgTable('node_api_users', {
  id: integer('id')
    .primaryKey()
    .default(sql`nextval('serial_node_api_users')`),
  name: text('name').notNull(),
});

let connection: DuckDBConnection;
let db: DuckDBDatabase;

beforeAll(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  connection = await instance.connect();
  db = drizzle(connection);

  await db.execute(sql`create sequence if not exists serial_node_api_users;`);
  await db.execute(sql`
    create table if not exists ${nodeUsers} (
      id integer primary key default nextval('serial_node_api_users'),
      name text not null
    )
  `);
});

beforeEach(async () => {
  await db.execute(sql`delete from ${nodeUsers}`);
});

afterAll(() => {
  connection?.closeSync();
});

test('@duckdb/node-api supports inserts and selects', async () => {
  await db.insert(nodeUsers).values([
    { name: 'Neo' },
    { name: 'Trinity' },
  ]);

  const rows = await db.select().from(nodeUsers).orderBy(nodeUsers.id);

  expect(rows).toHaveLength(2);
  expect(rows[0]?.name).toBe('Neo');
  expect(rows[1]?.name).toBe('Trinity');
});

test('transactions work with the node api connection', async () => {
  await db.transaction(async (tx) => {
    await tx.insert(nodeUsers).values({ name: 'Committed' });
  });

  await expect(
    db.transaction(async (tx) => {
      await tx.insert(nodeUsers).values({ name: 'Rolled back' });
      throw new Error('planned failure');
    })
  ).rejects.toThrow();

  const names = (
    await db.select({ name: nodeUsers.name }).from(nodeUsers)
  ).map((row) => row.name);

  expect(names).toContain('Committed');
  expect(names).not.toContain('Rolled back');
});
