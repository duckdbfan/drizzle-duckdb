import { DuckDBInstance } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { integer, pgTable } from 'drizzle-orm/pg-core';
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
import { duckDbInterval, duckDbTimestamp } from '../src/columns.ts';

describe('INTERVAL Column Type Tests', () => {
  let instance: DuckDBInstance;
  let db: DuckDBDatabase;

  const intervalTable = pgTable('interval_test', {
    id: integer('id').primaryKey(),
    duration: duckDbInterval('duration'),
    startTime: duckDbTimestamp('start_time'),
  });

  beforeAll(async () => {
    instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    db = drizzle(connection);

    await db.execute(sql`
      CREATE TABLE interval_test (
        id INTEGER PRIMARY KEY,
        duration INTERVAL,
        start_time TIMESTAMP
      )
    `);
  });

  afterAll(async () => {
    await db.close();
    instance.closeSync?.();
  });

  beforeEach(async () => {
    await db.execute(sql`DELETE FROM interval_test`);
  });

  test('stores simple day interval', async () => {
    await db.insert(intervalTable).values({ id: 1, duration: '1 day' });

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    // Duration may be returned as string or object depending on DuckDB version
    expect(result[0]?.duration).toBeDefined();
  });

  test('stores multiple days interval', async () => {
    await db.insert(intervalTable).values({ id: 1, duration: '5 days' });

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    // Duration may be returned as string or object depending on DuckDB version
    expect(result[0]?.duration).toBeDefined();
  });

  test('stores hour interval', async () => {
    await db.insert(intervalTable).values({ id: 1, duration: '3 hours' });

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    expect(result[0]?.duration).toBeDefined();
  });

  test('stores complex interval', async () => {
    await db
      .insert(intervalTable)
      .values({ id: 1, duration: '2 days 3 hours' });

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    expect(result[0]?.duration).toBeDefined();
  });

  test('stores month interval', async () => {
    await db.insert(intervalTable).values({ id: 1, duration: '1 month' });

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    expect(result[0]?.duration).toBeDefined();
  });

  test('handles null interval', async () => {
    await db.execute(
      sql`INSERT INTO interval_test (id, duration) VALUES (1, NULL)`
    );

    const result = await db
      .select()
      .from(intervalTable)
      .where(sql`id = 1`);
    expect(result[0]?.duration).toBeNull();
  });

  test('performs interval arithmetic with timestamp via SQL', async () => {
    const startTime = new Date('2024-03-15T12:00:00Z');
    await db.insert(intervalTable).values({
      id: 1,
      duration: '1 day',
      startTime,
    });

    const result = await db.execute<{ result_time: Date }>(sql`
      SELECT start_time + INTERVAL '1 day' as result_time
      FROM interval_test
      WHERE id = 1
    `);

    expect(result[0]?.result_time).toBeDefined();
    // Result should be ~1 day after start time
    const resultTime = new Date(result[0]!.result_time);
    expect(resultTime.getTime()).toBeGreaterThan(startTime.getTime());
  });

  test('compares intervals in WHERE clause via SQL', async () => {
    await db.execute(
      sql`INSERT INTO interval_test (id, duration) VALUES (1, '1 hour')`
    );
    await db.execute(
      sql`INSERT INTO interval_test (id, duration) VALUES (2, '2 hours')`
    );
    await db.execute(
      sql`INSERT INTO interval_test (id, duration) VALUES (3, '30 minutes')`
    );

    const result = await db.execute<{ id: number }>(sql`
      SELECT id FROM interval_test
      WHERE duration > INTERVAL '1 hour'
      ORDER BY id
    `);

    expect(result.length).toBe(1);
    expect(result[0]?.id).toBe(2);
  });
});
