import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { avgN, countN, drizzle, sumN } from '../src';
import type { DuckDBDatabase } from '../src';

const metrics = pgTable('olap_metrics', {
  id: integer('id').primaryKey(),
  value: integer('value').notNull(),
});

const batchItems = pgTable('olap_batch_items', {
  id: integer('id').primaryKey(),
  label: text('label').notNull(),
});

let connection: DuckDBConnection;
let db: DuckDBDatabase;

beforeAll(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  connection = await instance.connect();
  db = drizzle(connection);

  await db.execute(sql`
    create table if not exists ${metrics} (
      id integer primary key,
      value integer not null
    )
  `);

  await db.execute(sql`
    create table if not exists ${batchItems} (
      id integer primary key,
      label text not null
    )
  `);
});

beforeEach(async () => {
  await db.execute(sql`delete from ${metrics}`);
  await db.execute(sql`delete from ${batchItems}`);
});

afterAll(() => {
  connection?.closeSync();
});

test('numeric helpers coerce aggregates to numbers', async () => {
  await db.insert(metrics).values([
    { id: 1, value: 10 },
    { id: 2, value: 20 },
    { id: 3, value: 30 },
  ]);

  const [row] = await db
    .select({
      total: sumN(metrics.value),
      average: avgN(metrics.value),
      count: countN(),
    })
    .from(metrics);

  expect(row.total).toBe(60);
  expect(row.average).toBe(20);
  expect(row.count).toBe(3);
});

test('executeBatches yields chunks without materializing everything', async () => {
  const rowsToInsert = Array.from({ length: 12 }, (_, idx) => ({
    id: idx,
    label: `item-${idx}`,
  }));
  await db.insert(batchItems).values(rowsToInsert);

  const chunkSizes: number[] = [];
  const labels: string[] = [];

  for await (const chunk of db.executeBatches(
    sql`select id, label from ${batchItems} order by id`,
    { rowsPerChunk: 5 }
  )) {
    chunkSizes.push(chunk.length);
    labels.push(...chunk.map((row) => row.label as string));
  }

  expect(chunkSizes).toEqual([5, 5, 2]);
  expect(labels.slice(0, 3)).toEqual(['item-0', 'item-1', 'item-2']);
  expect(labels).toHaveLength(12);
});
