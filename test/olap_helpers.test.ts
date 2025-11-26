import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import {
  anyValue,
  avgN,
  denseRank,
  drizzle,
  lag,
  median,
  olap,
  percentileCont,
  rowNumber,
  sumN,
} from '../src';
import type { DuckDBDatabase } from '../src';

const numbers = pgTable('olap_numbers', {
  id: integer('id').primaryKey(),
  val: integer('val').notNull(),
});

const windowed = pgTable('olap_windowed', {
  id: integer('id').primaryKey(),
  amount: integer('amount').notNull(),
});

const sales = pgTable('olap_sales', {
  region: text('region').notNull(),
  product: text('product').notNull(),
  qty: integer('qty').notNull(),
});

let connection: DuckDBConnection;
let db: DuckDBDatabase;

beforeAll(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  connection = await instance.connect();
  db = drizzle(connection);

  await db.execute(sql`
    create table if not exists ${numbers} (
      id integer primary key,
      val integer not null
    )
  `);

  await db.execute(sql`
    create table if not exists ${windowed} (
      id integer primary key,
      amount integer not null
    )
  `);

  await db.execute(sql`
    create table if not exists ${sales} (
      region text not null,
      product text not null,
      qty integer not null
    )
  `);
});

beforeEach(async () => {
  await db.execute(sql`delete from ${numbers}`);
  await db.execute(sql`delete from ${windowed}`);
  await db.execute(sql`delete from ${sales}`);
});

afterAll(() => {
  connection?.closeSync();
});

test('percentileCont and median return numbers', async () => {
  await db.insert(numbers).values([
    { id: 1, val: 10 },
    { id: 2, val: 20 },
    { id: 3, val: 30 },
    { id: 4, val: 40 },
    { id: 5, val: 50 },
  ]);

  const [row] = await db
    .select({
      p50: percentileCont(0.5, numbers.val),
      med: median(numbers.val),
    })
    .from(numbers);

  expect(row.p50).toBe(30);
  expect(row.med).toBe(30);
});

test('window helpers: rowNumber, denseRank, lag', async () => {
  await db.insert(windowed).values([
    { id: 1, amount: 5 },
    { id: 2, amount: 10 },
    { id: 3, amount: 10 },
    { id: 4, amount: 20 },
  ]);

  const rows = await db
    .select({
      id: windowed.id,
      rn: rowNumber({ orderBy: windowed.id }),
      dr: denseRank({ orderBy: windowed.amount }),
      prevAmount: lag<number>(windowed.amount, 1, sql`-1`, {
        orderBy: windowed.id,
      }),
    })
    .from(windowed)
    .orderBy(windowed.id);

  expect(rows.map((r) => r.rn)).toEqual([1, 2, 3, 4]);
  expect(rows.map((r) => r.dr)).toEqual([1, 2, 2, 3]);
  expect(rows.map((r) => r.prevAmount)).toEqual([-1, 5, 10, 10]);
});

test('olap builder injects any_value for non-aggregated selections', async () => {
  await db.insert(sales).values([
    { region: 'west', product: 'widget', qty: 2 },
    { region: 'west', product: 'gadget', qty: 3 },
    { region: 'east', product: 'widget', qty: 1 },
  ]);

  const rows = await olap(db)
    .from(sales)
    .groupBy([sales.region])
    .selectNonAggregates({ sampleProduct: sales.product }, { anyValue: true })
    .measures({
      totalQty: sumN(sales.qty),
      avgQty: avgN(sales.qty),
    })
    .orderBy(sales.region)
    .run();

  expect(rows).toHaveLength(2);

  const west = rows.find((r) => r['olap_sales.region'] === 'west');
  const east = rows.find((r) => r['olap_sales.region'] === 'east');

  expect(west?.totalQty).toBe(5);
  expect(east?.totalQty).toBe(1);
  expect(west?.sampleProduct).toBeDefined();
  expect(east?.sampleProduct).toBeDefined();
});
