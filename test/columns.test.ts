import { Database } from 'duckdb-async';
import {
  drizzle,
  DuckDBDatabase,
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
import { assert, beforeAll, beforeEach, test } from 'vitest';

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
  struct_data: duckDbStruct<{ name: string; age: number }>('struct_data', {
    name: 'STRING',
    age: 'INTEGER',
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
  client: Database;
  structTable: typeof structTable;
}

let ctx: Context;

beforeAll(async () => {
  const client = await Database.create(':memory:');

  const db = drizzle(client, { logger: ENABLE_LOGGING });

  ctx = {
    client,
    db,
    structTable,
  };

  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS "struct_table" (
      "id" integer PRIMARY KEY NOT NULL,
      "struct_data" STRUCT (name STRING, age INTEGER) NOT NULL,
      "nested_list_struct" STRUCT (name STRING, age INTEGER, favorite_numbers INTEGER[]),
    );
  `);

  await db.insert(structTable).values({
    id: 1,
    struct_data: { name: 'Carmac, John', age: 30 },
    nested_list_struct: {
      name: 'Carmac, John',
      age: 30,
      favorite_numbers: [0, 1, 42],
    },
  });
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
  await db.execute(sql`drop table if exists "drizzle"."__drizzle_migrations"`);

  await db.execute(sql`create schema drizzle`);
  await db.execute(sql.raw('CREATE SEQUENCE IF NOT EXISTS migrations_pk_seq'));
  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS "drizzle"."__drizzle_migrations" (
          id INTEGER PRIMARY KEY default nextval('migrations_pk_seq'),
          hash text NOT NULL,
          created_at bigint
        )
    `);

  await migrate(db, { migrationsFolder: './test/duckdb/pg' });

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
  await db.execute(sql`drop table if exists "drizzle"."__drizzle_migrations"`);
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
