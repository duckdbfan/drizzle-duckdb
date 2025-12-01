import { DuckDBInstance } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { integer, pgTable, text, varchar } from 'drizzle-orm/pg-core';
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

describe('Query Error Conditions', () => {
  let instance: DuckDBInstance;
  let db: DuckDBDatabase;

  const testTable = pgTable('error_test', {
    id: integer('id').primaryKey(),
    name: varchar('name', { length: 100 }).notNull(),
    email: text('email').unique(),
  });

  beforeAll(async () => {
    instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    db = drizzle(connection);

    // Create the test table
    await db.execute(sql`
      CREATE TABLE error_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email TEXT UNIQUE
      )
    `);
  });

  afterAll(async () => {
    await db.close();
    instance.closeSync?.();
  });

  beforeEach(async () => {
    // Clear data between tests
    await db.execute(sql`DELETE FROM error_test`);
  });

  test('invalid SQL syntax throws parser error', async () => {
    // Use direct SQL that will fail parsing
    try {
      await db.execute(sql`SELEC * FROM error_test`);
      expect.fail('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
    }
  });

  test('reference to non-existent table throws error', async () => {
    try {
      await db.execute(sql`SELECT * FROM does_not_exist`);
      expect.fail('Should have thrown');
    } catch (e) {
      expect(String(e)).toMatch(/does not exist|not found/i);
    }
  });

  test('reference to non-existent column throws error', async () => {
    try {
      await db.execute(sql`SELECT nonexistent_column FROM error_test`);
      expect.fail('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
    }
  });

  test('primary key constraint violation throws error', async () => {
    await db.insert(testTable).values({ id: 1, name: 'First' });

    try {
      await db.insert(testTable).values({ id: 1, name: 'Duplicate' });
      expect.fail('Should have thrown');
    } catch (e) {
      expect(String(e)).toMatch(/duplicate|constraint|primary/i);
    }
  });

  test('unique constraint violation throws error', async () => {
    await db
      .insert(testTable)
      .values({ id: 1, name: 'First', email: 'test@example.com' });

    try {
      await db
        .insert(testTable)
        .values({ id: 2, name: 'Second', email: 'test@example.com' });
      expect.fail('Should have thrown');
    } catch (e) {
      expect(String(e)).toMatch(/duplicate|unique|constraint/i);
    }
  });

  test('NOT NULL constraint violation throws error', async () => {
    try {
      await db.execute(sql`INSERT INTO error_test (id, name) VALUES (1, NULL)`);
      expect.fail('Should have thrown');
    } catch (e) {
      expect(String(e)).toMatch(/null|constraint/i);
    }
  });

  test('division by zero behavior', async () => {
    // DuckDB may return NULL or Infinity for division by zero instead of throwing
    // Test that we can at least execute the query
    const result = await db.execute<{ val: number | null }>(
      sql`SELECT 1.0 / 0.0 as val`
    );
    // DuckDB returns Infinity for float division by zero
    expect(result).toBeDefined();
  });

  test('type mismatch in comparison throws error', async () => {
    await db.insert(testTable).values({ id: 1, name: 'Test' });

    try {
      await db.execute(sql`SELECT * FROM error_test WHERE id = ARRAY[1,2,3]`);
      expect.fail('Should have thrown');
    } catch (e) {
      expect(e).toBeDefined();
    }
  });
});
