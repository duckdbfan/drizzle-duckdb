import { Database } from 'duckdb-async';
import { drizzle, DuckDBDatabase } from '../src';
import { alias, boolean, char, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core';
import { DefaultLogger, eq, sql } from 'drizzle-orm';
import { assert, beforeAll, beforeEach, test } from 'vitest';

console.log('assert');
console.log(assert);

const ENABLE_LOGGING = false;

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

interface Context {
  db: DuckDBDatabase;
  client: Database;
}

let ctx: Context;

beforeAll(async () => {
  const client = await Database.create(':memory:');

  const db = drizzle(client, { logger: ENABLE_LOGGING });

  ctx = {
    client,
    db,
  };

  await db.execute(sql`CREATE SEQUENCE serial_users;`);
  await db.execute(sql`CREATE SEQUENCE serial_users2;`);
  await db.execute(sql`CREATE SEQUENCE serial_cities;`);

  await db.execute(
    sql`
    create table if not exists cities (
      id integer primary key default nextval('serial_cities'),
      name text not null,
      state char(2)
    )
  `
  );

  await db.execute(
    sql`
    create table if not exists users2 (
      id integer primary key default nextval('serial_users2'),
      name text not null,
      city_id integer references cities(id)
    )
  `
  );
});

test('return null instead of object if join has no match', async () => {
  const { id: cityId } = await ctx.db
    .insert(citiesTable)
    .values([
      { id: 1, name: 'Paris' },
      { id: 2, name: 'London' },
    ])
    .returning({ id: citiesTable.id })
    .then((rows) => rows[0]!);

  await ctx.db.insert(users2Table).values([
    { id: 1, name: 'John', cityId },
    { id: 2, name: 'Jane' },
  ]);

  const res = await ctx.db
    .select({
      id: users2Table.id,
      user: {
        name: users2Table.name,
        nameUpper: sql<string>`upper(${users2Table.name})`,
      },
      city: {
        id: citiesTable.id,
        name: citiesTable.name,
        nameUpper: sql<string>`upper(${citiesTable.name})`,
      },
    })
    .from(users2Table)
    .leftJoin(citiesTable, eq(users2Table.cityId, citiesTable.id));

  assert.deepEqual(res, [
    {
      id: 1,
      user: {
        name: 'John',
        nameUpper: 'JOHN',
      },
      city: {
        id: 1,
        name: 'Paris',
        nameUpper: 'PARIS',
      },
    },
    {
      id: 2,
      user: {
        name: 'Jane',
        nameUpper: 'JANE',
      },
      city: null,
    },
  ]);
});
