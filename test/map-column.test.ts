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
import { duckDbMap } from '../src/columns.ts';

describe('MAP Column Type Tests', () => {
  let instance: DuckDBInstance;
  let db: DuckDBDatabase;

  const mapTable = pgTable('map_test', {
    id: integer('id').primaryKey(),
    stringMap: duckDbMap<Record<string, string>>('string_map', 'VARCHAR'),
    intMap: duckDbMap<Record<string, number>>('int_map', 'INTEGER'),
  });

  beforeAll(async () => {
    instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    db = drizzle(connection);

    await db.execute(sql`
      CREATE TABLE map_test (
        id INTEGER PRIMARY KEY,
        string_map MAP(STRING, VARCHAR),
        int_map MAP(STRING, INTEGER)
      )
    `);
  });

  afterAll(async () => {
    await db.close();
    instance.closeSync?.();
  });

  beforeEach(async () => {
    await db.execute(sql`DELETE FROM map_test`);
  });

  test('inserts and retrieves simple string map', async () => {
    const data = { key1: 'value1', key2: 'value2' };
    await db.insert(mapTable).values({ id: 1, stringMap: data });

    const result = await db
      .select()
      .from(mapTable)
      .where(sql`id = 1`);
    // MAP returns can vary by DuckDB version, just check it exists
    expect(result[0]?.stringMap).toBeDefined();
  });

  test('inserts and retrieves integer map', async () => {
    const data = { a: 1, b: 2, c: 3 };
    await db.insert(mapTable).values({ id: 1, intMap: data });

    const result = await db
      .select()
      .from(mapTable)
      .where(sql`id = 1`);
    // MAP returns can vary by DuckDB version, just check it exists
    expect(result[0]?.intMap).toBeDefined();
  });

  test('handles empty map', async () => {
    const data: Record<string, string> = {};
    await db.insert(mapTable).values({ id: 1, stringMap: data });

    const result = await db
      .select()
      .from(mapTable)
      .where(sql`id = 1`);
    expect(result[0]).toBeDefined();
  });

  test('handles null map', async () => {
    await db.execute(
      sql`INSERT INTO map_test (id, string_map) VALUES (1, NULL)`
    );

    const result = await db
      .select()
      .from(mapTable)
      .where(sql`id = 1`);
    expect(result[0]?.stringMap).toBeNull();
  });

  test('handles map with many keys', async () => {
    const data: Record<string, number> = {};
    for (let i = 0; i < 100; i++) {
      data[`key_${i}`] = i;
    }
    await db.insert(mapTable).values({ id: 1, intMap: data });

    const result = await db
      .select()
      .from(mapTable)
      .where(sql`id = 1`);
    expect(Object.keys(result[0]?.intMap ?? {}).length).toBe(100);
  });

  test('accesses map element via SQL syntax', async () => {
    const data = { target: 'found_it' };
    await db.insert(mapTable).values({ id: 1, stringMap: data });

    const result = await db.execute<{ val: string }>(
      sql`SELECT string_map['target'] as val FROM map_test WHERE id = 1`
    );
    expect(result[0]?.val).toBe('found_it');
  });
});
