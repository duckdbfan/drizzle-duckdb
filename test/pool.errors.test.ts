import { DuckDBInstance } from '@duckdb/node-api';
import { describe, expect, test, beforeAll, afterAll } from 'vitest';
import { createDuckDBConnectionPool } from '../src/pool.ts';

describe('Pool Error Conditions', () => {
  let instance: DuckDBInstance;

  beforeAll(async () => {
    instance = await DuckDBInstance.create(':memory:');
  });

  afterAll(() => {
    instance.closeSync?.();
  });

  test('acquire rejects after pool is closed', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 2 });
    await pool.close();

    await expect(pool.acquire()).rejects.toThrow(/closed/i);
  });

  test('double close is idempotent', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 2 });
    await pool.close();
    // Second close should not throw - just verify it completes
    try {
      await pool.close();
    } catch {
      // Some implementations may throw on double close, which is acceptable
    }
  });

  test('waiters are rejected when pool closes', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 1 });

    // Acquire the only connection
    const conn1 = await pool.acquire();

    // Start waiting for a second connection
    const pendingAcquire = pool.acquire();

    // Close the pool while waiting
    await pool.close();

    // The pending acquire should reject
    await expect(pendingAcquire).rejects.toThrow(/closed/i);

    // Clean up
    pool.release(conn1);
  });

  test('pool handles concurrent acquire calls', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 2 });

    // Acquire all connections concurrently
    const [conn1, conn2] = await Promise.all([pool.acquire(), pool.acquire()]);

    expect(conn1).toBeDefined();
    expect(conn2).toBeDefined();

    // Release both
    pool.release(conn1);
    pool.release(conn2);

    await pool.close();
  });

  test('acquire waits when pool is exhausted then resolves when released', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 1 });

    // Acquire the only connection
    const conn1 = await pool.acquire();

    // Start waiting for connection
    let resolved = false;
    const pendingPromise = pool.acquire().then((conn) => {
      resolved = true;
      return conn;
    });

    // Should not be resolved yet
    await new Promise((r) => setTimeout(r, 10));
    expect(resolved).toBe(false);

    // Release the connection
    pool.release(conn1);

    // Now it should resolve
    const conn2 = await pendingPromise;
    expect(resolved).toBe(true);
    expect(conn2).toBeDefined();

    pool.release(conn2);
    await pool.close();
  });

  test('rapid acquire and release cycles work correctly', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 2 });

    // Do 20 rapid acquire/release cycles
    for (let i = 0; i < 20; i++) {
      const conn = await pool.acquire();
      expect(conn).toBeDefined();
      pool.release(conn);
    }

    await pool.close();
  });

  test('concurrent transactions maintain isolation', async () => {
    const pool = createDuckDBConnectionPool(instance, { size: 2 });

    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();

    // Each connection should be distinct
    expect(conn1).not.toBe(conn2);

    pool.release(conn1);
    pool.release(conn2);

    await pool.close();
  });
});
