import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { integer, pgTable } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { duckDbJson, drizzle, type DuckDBDatabase } from '../src';

const jsonTable = pgTable('duckdb_json_items', {
  id: integer('id').primaryKey(),
  payload: duckDbJson<{
    foo: string;
    nested: { count: number };
    tags: string[];
  }>('payload'),
});

interface Context {
  db: DuckDBDatabase;
  connection: DuckDBConnection;
}

let ctx: Context;

beforeAll(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const db = drizzle(connection);

  ctx = { db, connection };

  await db.execute(sql`drop table if exists ${jsonTable}`);
  await db.execute(sql`
    create table ${jsonTable} (
      id integer primary key,
      payload json
    )
  `);
});

beforeEach(async () => {
  await ctx.db.execute(sql`delete from ${jsonTable}`);
  await ctx.db.insert(jsonTable).values({
    id: 1,
    payload: {
      foo: 'bar',
      nested: { count: 1 },
      tags: ['duck', 'db'],
    },
  });
});

afterAll(() => {
  ctx.connection?.closeSync();
});

test('duckDbJson round-trips objects', async () => {
  const rows = await ctx.db
    .select()
    .from(jsonTable)
    .where(sql`${jsonTable.id} = 1`);
  expect(rows[0]?.payload).toEqual({
    foo: 'bar',
    nested: { count: 1 },
    tags: ['duck', 'db'],
  });
});

test('json operators and functions work with duckDbJson', async () => {
  const [{ nested_count: nestedCount, foo_text: fooText }] =
    await ctx.db.execute(
      sql`
      select
        json_extract(${jsonTable.payload}, '$.nested.count') as nested_count,
        ${jsonTable.payload} ->> 'foo' as foo_text
      from ${jsonTable}
      where ${jsonTable.id} = 1
    `
    );

  expect(Number(nestedCount)).toBe(1);
  expect(fooText).toBe('bar');
});
