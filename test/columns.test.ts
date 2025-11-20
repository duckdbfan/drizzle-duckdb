import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import {
  drizzle,
  DuckDBDatabase,
  duckDbArray,
  duckDbArrayContains,
  duckDbMap,
  duckDbStruct,
  migrate,
} from '../src';
import {
  alias,
  boolean,
  char,
  integer,
  pgTable,
  text,
  timestamp,
} from 'drizzle-orm/pg-core';
import { DefaultLogger, eq, sql } from 'drizzle-orm';
import assert from 'node:assert/strict';
import { afterAll, beforeAll, test } from 'vitest';

const ENABLE_LOGGING = true;

const citiesTable = pgTable('cities', {
  id: integer('id')
    .primaryKey()
    .default(sql`nextval('serial_cities')`),
  name: text('name').notNull(),
  state: char('state', { length: 2 }),
});

const users2Table = pgTable('users2', {
  id: integer('id')
    .primaryKey()
    .default(sql`nextval('serial_users2')`),
  name: text('name').notNull(),
  cityId: integer('city_id').references(() => citiesTable.id),
});

const structTable = pgTable('struct_table', {
  id: integer('id').primaryKey(),
  struct_data: duckDbStruct<{ name: string; age: number; favorite_numbers: number[] }>('struct_data', {
    name: 'STRING',
    age: 'INTEGER',
    favorite_numbers: 'INTEGER[]',
  }),
  nested_list_struct: duckDbStruct<{
    name: string;
    age: number;
    favorite_numbers: number[];
  }>('nested_list_struct', {
    name: 'STRING',
    age: 'INTEGER',
    favorite_numbers: 'INTEGER[]',
  }),
});

interface Context {
  db: DuckDBDatabase;
  structTable: typeof structTable;
  connection: DuckDBConnection;
}

let ctx: Context;

beforeAll(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const db = drizzle(connection, { logger: ENABLE_LOGGING });

  ctx = {
    db,
    structTable,
    connection,
  };

  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS "struct_table" (
      "id" integer PRIMARY KEY NOT NULL,
      "struct_data" STRUCT (name STRING, age INTEGER, favorite_numbers INTEGER[]) NOT NULL,
      "nested_list_struct" STRUCT (name STRING, age INTEGER, favorite_numbers INTEGER[])
    );
  `);

  await db.insert(structTable).values({
    id: 1,
    struct_data: { name: 'Carmac, John', age: 30, favorite_numbers: [0, 1, 2] },
    nested_list_struct: {
      name: 'Carmac, John',
      age: 30,
      favorite_numbers: [0, 1, 42],
    },
  });
});

afterAll(() => {
  ctx.connection?.closeSync();
});

test('migration', async () => {
  const { db } = ctx;

  const structTable = pgTable('duckdb_cols', {
    id: integer('id').primaryKey(),
    data: duckDbStruct<{ name: string; age: number }>('struct_string', {
      name: 'STRING',
      age: 'INTEGER',
    }),
  });

  await db.execute(sql`drop table if exists duckdb_cols`);
  await db.execute(sql`drop schema if exists drizzle cascade`);

  await migrate(db, './test/duckdb/pg');

  const sequences = await db.execute<{ sequencename: string }>(
    sql`select sequencename from pg_catalog.pg_sequences where schemaname = 'drizzle' and sequencename = '__drizzle_migrations_id_seq'`
  );
  assert.equal(sequences.length, 1);

  await db.insert(structTable).values({
    id: 1,
    data: { name: 'Carmac, John', age: 30 },
  });

  const result = await db
    .select({ id: structTable.id, data: structTable.data })
    .from(structTable);

  assert.deepEqual(result, [
    { id: 1, data: { name: 'Carmac, John', age: 30 } },
  ]);

  await db.execute(sql`drop table if exists duckdb_cols`);
  await db.execute(sql`drop schema if exists drizzle cascade`);
});

test('struct column: property access by name', async () => {
  const { db } = ctx;

  const result2 = await db
    .select({
      id: structTable.id,
      name: sql<string>`${structTable.struct_data}['name']`,
    })
    .from(structTable);

  assert.deepEqual(result2, [{ id: 1, name: 'Carmac, John' }]);
});

test('struct column: nested list property', async () => {
  const { db } = ctx;

  const result2 = await db
    .select({
      id: structTable.id,
      favorite_numbers: sql<
        number[]
      >`${structTable.nested_list_struct}['favorite_numbers']`,
    })
    .from(structTable);

  assert.deepEqual(result2, [{ id: 1, favorite_numbers: [0, 1, 42] }]);
});

test('struct + arrays via custom helper', async () => {
  const { db } = ctx;

  const arraysTable = pgTable('duckdb_arrays', {
    id: integer('id').primaryKey(),
    numbers: duckDbArray<number>('numbers', 'INTEGER'),
    payload: duckDbStruct<{ meta: string; bits: number[] }>('payload', {
      meta: 'TEXT',
      bits: 'INTEGER[]',
    }),
  });

  await db.execute(sql`drop table if exists ${arraysTable}`);
  await db.execute(sql`
    create table ${arraysTable} (
      id integer primary key,
      numbers integer[],
      payload struct(meta text, bits integer[])
    )
  `);

  await db.insert(arraysTable).values({
    id: 1,
    numbers: [1, 2, 3],
    payload: { meta: 'hi', bits: [7, 8] },
  });

  const rows = await db
    .select({ id: arraysTable.id, numbers: arraysTable.numbers, payload: arraysTable.payload })
    .from(arraysTable);

  assert.deepEqual(rows, [
    { id: 1, numbers: [1, 2, 3], payload: { meta: 'hi', bits: [7, 8] } },
  ]);

  const contains = await db
    .select({ id: arraysTable.id })
    .from(arraysTable)
    .where(duckDbArrayContains(arraysTable.numbers, [2]));

  assert.deepEqual(contains, [{ id: 1 }]);
});
