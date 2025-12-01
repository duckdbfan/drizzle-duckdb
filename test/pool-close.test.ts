import { DuckDBInstance } from '@duckdb/node-api';
import { expect, test } from 'vitest';
import { createDuckDBConnectionPool } from '../src';

test('pending acquires reject when pool closes', async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const pool = createDuckDBConnectionPool(instance, { size: 1 });

  const conn1 = await pool.acquire();
  const pending = pool.acquire();

  await pool.close();
  await pool.release(conn1);

  await expect(pending).rejects.toThrow(/closed/);
  await expect(pool.acquire()).rejects.toThrow(/closed/);

  instance.closeSync?.();
});
