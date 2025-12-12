import { DuckDBInstance } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import {
  integer,
  bigint,
  text,
  doublePrecision,
  pgTable,
} from 'drizzle-orm/pg-core';
import {
  describe,
  expect,
  test,
  beforeAll,
  afterAll,
  beforeEach,
} from 'vitest';
import { drizzle } from '../src/driver.ts';
import type { DuckDBDatabase } from '../src/driver.ts';
import { duckDbTimestamp } from '../src/columns.ts';

describe('Boundary Value Tests', () => {
  let instance: DuckDBInstance;
  let db: DuckDBDatabase;

  const boundaryTable = pgTable('boundary_test', {
    id: integer('id').primaryKey(),
    intVal: integer('int_val'),
    bigintVal: bigint('bigint_val', { mode: 'number' }),
    textVal: text('text_val'),
    floatVal: doublePrecision('float_val'),
    timestampVal: duckDbTimestamp('timestamp_val'),
  });

  beforeAll(async () => {
    instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    db = drizzle(connection);

    await db.execute(sql`
      CREATE TABLE boundary_test (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        bigint_val BIGINT,
        text_val TEXT,
        float_val DOUBLE,
        timestamp_val TIMESTAMP
      )
    `);
  });

  afterAll(async () => {
    await db.close();
    instance.closeSync?.();
  });

  beforeEach(async () => {
    await db.execute(sql`DELETE FROM boundary_test`);
  });

  test('INTEGER MAX value stores and retrieves correctly', async () => {
    const maxInt = 2147483647; // 2^31 - 1
    await db.insert(boundaryTable).values({ id: 1, intVal: maxInt });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.intVal).toBe(maxInt);
  });

  test('INTEGER MIN value stores and retrieves correctly', async () => {
    const minInt = -2147483648; // -2^31
    await db.insert(boundaryTable).values({ id: 1, intVal: minInt });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.intVal).toBe(minInt);
  });

  test('BIGINT stores small positive value correctly', async () => {
    const smallBigint = 1234567890;
    await db.insert(boundaryTable).values({ id: 1, bigintVal: smallBigint });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    // BIGINT may be returned as bigint type
    expect(Number(result[0]?.bigintVal)).toBe(smallBigint);
  });

  test('BIGINT stores small negative value correctly', async () => {
    const smallBigint = -1234567890;
    await db.insert(boundaryTable).values({ id: 1, bigintVal: smallBigint });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    // BIGINT may be returned as bigint type
    expect(Number(result[0]?.bigintVal)).toBe(smallBigint);
  });

  test('empty string stores as empty string, not NULL', async () => {
    await db.insert(boundaryTable).values({ id: 1, textVal: '' });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.textVal).toBe('');
    expect(result[0]?.textVal).not.toBeNull();
  });

  test('NULL stores and retrieves as null', async () => {
    await db.insert(boundaryTable).values({ id: 1, textVal: null });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.textVal).toBeNull();
  });

  test('timestamp at epoch (1970-01-01) stores correctly', async () => {
    const epoch = new Date('1970-01-01T00:00:00.000Z');
    await db.insert(boundaryTable).values({ id: 1, timestampVal: epoch });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.timestampVal).toBeInstanceOf(Date);
    expect((result[0]?.timestampVal as Date).getTime()).toBe(0);
  });

  test('timestamp far in future stores correctly', async () => {
    const future = new Date('2099-12-31T23:59:59.999Z');
    await db.insert(boundaryTable).values({ id: 1, timestampVal: future });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.timestampVal).toBeInstanceOf(Date);
    expect((result[0]?.timestampVal as Date).getUTCFullYear()).toBe(2099);
  });

  test('DOUBLE precision handles very small numbers', async () => {
    const tiny = 1e-308;
    await db.insert(boundaryTable).values({ id: 1, floatVal: tiny });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.floatVal).toBeCloseTo(tiny, 300);
  });

  test('DOUBLE precision handles large numbers', async () => {
    const large = 12345678901234.5;
    await db.insert(boundaryTable).values({ id: 1, floatVal: large });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    // Check value was stored correctly
    expect(result[0]?.floatVal).toBeCloseTo(large, 1);
  });

  test('zero values store correctly', async () => {
    await db.insert(boundaryTable).values({
      id: 1,
      intVal: 0,
      bigintVal: 0,
      floatVal: 0.0,
    });

    const result = await db
      .select()
      .from(boundaryTable)
      .where(sql`id = 1`);
    expect(result[0]?.intVal).toBe(0);
    expect(result[0]?.bigintVal).toBe(0);
    expect(result[0]?.floatVal).toBe(0);
  });
});
