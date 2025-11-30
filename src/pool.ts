import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { closeClientConnection, type DuckDBConnectionPool } from './client.ts';

/** Pool size presets for different MotherDuck instance types */
export type PoolPreset =
  | 'pulse'
  | 'standard'
  | 'jumbo'
  | 'mega'
  | 'giga'
  | 'local'
  | 'memory';

/** Pool sizes optimized for each MotherDuck instance type */
export const POOL_PRESETS: Record<PoolPreset, number> = {
  pulse: 4, // Auto-scaling, ad-hoc analytics
  standard: 6, // Balanced ETL/ELT workloads
  jumbo: 8, // Complex queries, high-volume
  mega: 12, // Large-scale transformations
  giga: 16, // Maximum parallelism
  local: 8, // Local DuckDB file
  memory: 4, // In-memory testing
};

export interface DuckDBPoolConfig {
  /** Maximum concurrent connections. Defaults to 4. */
  size?: number;
}

/**
 * Resolve pool configuration to a concrete size.
 * Returns false if pooling is disabled.
 */
export function resolvePoolSize(
  pool: DuckDBPoolConfig | PoolPreset | false | undefined
): number | false {
  if (pool === false) return false;
  if (pool === undefined) return 4;
  if (typeof pool === 'string') return POOL_PRESETS[pool];
  return pool.size ?? 4;
}

export interface DuckDBConnectionPoolOptions {
  /** Maximum concurrent connections. Defaults to 4. */
  size?: number;
}

export function createDuckDBConnectionPool(
  instance: DuckDBInstance,
  options: DuckDBConnectionPoolOptions = {}
): DuckDBConnectionPool & { size: number } {
  const size = options.size && options.size > 0 ? options.size : 4;
  const idle: DuckDBConnection[] = [];
  const waiting: Array<(conn: DuckDBConnection) => void> = [];
  let total = 0;
  let closed = false;

  const acquire = async (): Promise<DuckDBConnection> => {
    if (closed) {
      throw new Error('DuckDB connection pool is closed');
    }

    if (idle.length > 0) {
      return idle.pop() as DuckDBConnection;
    }

    if (total < size) {
      total += 1;
      return await DuckDBConnection.create(instance);
    }

    return await new Promise((resolve) => {
      waiting.push(resolve);
    });
  };

  const release = async (connection: DuckDBConnection): Promise<void> => {
    if (closed) {
      await closeClientConnection(connection);
      return;
    }

    const waiter = waiting.shift();
    if (waiter) {
      waiter(connection);
      return;
    }

    idle.push(connection);
  };

  const close = async (): Promise<void> => {
    closed = true;
    const toClose = idle.splice(0, idle.length);
    await Promise.all(toClose.map((conn) => closeClientConnection(conn)));
  };

  return {
    acquire,
    release,
    close,
    size,
  };
}
