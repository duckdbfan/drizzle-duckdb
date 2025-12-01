import {
  listValue,
  timestampValue,
  type DuckDBConnection,
  type DuckDBValue,
} from '@duckdb/node-api';
import {
  DUCKDB_VALUE_MARKER,
  wrapperToNodeApiValue,
  type AnyDuckDBValueWrapper,
} from './value-wrappers.ts';

export type DuckDBClientLike = DuckDBConnection | DuckDBConnectionPool;
export type RowData = Record<string, unknown>;

export interface DuckDBConnectionPool {
  acquire(): Promise<DuckDBConnection>;
  release(connection: DuckDBConnection): void | Promise<void>;
  close?(): Promise<void> | void;
}

export function isPool(
  client: DuckDBClientLike
): client is DuckDBConnectionPool {
  return typeof (client as DuckDBConnectionPool).acquire === 'function';
}

export interface PrepareParamsOptions {
  rejectStringArrayLiterals?: boolean;
  warnOnStringArrayLiteral?: () => void;
}

function isPgArrayLiteral(value: string): boolean {
  return value.startsWith('{') && value.endsWith('}');
}

function parsePgArrayLiteral(value: string): unknown {
  const json = value.replace(/{/g, '[').replace(/}/g, ']');

  try {
    return JSON.parse(json);
  } catch {
    return value;
  }
}

export function prepareParams(
  params: unknown[],
  options: PrepareParamsOptions = {}
): unknown[] {
  return params.map((param) => {
    if (typeof param === 'string') {
      const trimmed = param.trim();
      if (trimmed && isPgArrayLiteral(trimmed)) {
        if (options.rejectStringArrayLiterals) {
          throw new Error(
            'Stringified array literals are not supported. Use duckDbList()/duckDbArray() or pass native arrays.'
          );
        }

        if (options.warnOnStringArrayLiteral) {
          options.warnOnStringArrayLiteral();
        }
        return parsePgArrayLiteral(trimmed);
      }
    }
    return param;
  });
}

/**
 * Convert a value to DuckDB Node API value.
 * Handles wrapper types and plain values for backward compatibility.
 * Optimized for the common case (primitives) in the hot path.
 */
function toNodeApiValue(value: unknown): DuckDBValue {
  // Fast path 1: null/undefined
  if (value == null) return null;

  // Fast path 2: primitives (most common)
  const t = typeof value;
  if (t === 'string' || t === 'number' || t === 'bigint' || t === 'boolean') {
    return value as DuckDBValue;
  }

  // Fast path 3: pre-wrapped DuckDB value (Symbol check ~2-3ns)
  if (t === 'object' && DUCKDB_VALUE_MARKER in (value as object)) {
    return wrapperToNodeApiValue(
      value as AnyDuckDBValueWrapper,
      toNodeApiValue
    );
  }

  // Legacy path: plain arrays (backward compatibility)
  if (Array.isArray(value)) {
    return listValue(value.map((inner) => toNodeApiValue(inner)));
  }

  // Date conversion to timestamp
  if (value instanceof Date) {
    return timestampValue(BigInt(value.getTime()) * 1000n);
  }

  // Fallback for unknown objects
  return value as DuckDBValue;
}

function deduplicateColumns(columns: string[]): string[] {
  const seen: Record<string, number> = {};
  return columns.map((col) => {
    const count = seen[col] ?? 0;
    seen[col] = count + 1;
    return count === 0 ? col : `${col}_${count}`;
  });
}

function mapRowsToObjects(columns: string[], rows: unknown[][]): RowData[] {
  return rows.map((vals) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, idx) => {
      obj[col] = vals[idx];
    });
    return obj;
  }) as RowData[];
}

export async function closeClientConnection(
  connection: DuckDBConnection
): Promise<void> {
  if ('close' in connection && typeof connection.close === 'function') {
    await connection.close();
    return;
  }

  if ('closeSync' in connection && typeof connection.closeSync === 'function') {
    connection.closeSync();
    return;
  }

  if (
    'disconnectSync' in connection &&
    typeof connection.disconnectSync === 'function'
  ) {
    connection.disconnectSync();
  }
}

export async function executeOnClient(
  client: DuckDBClientLike,
  query: string,
  params: unknown[]
): Promise<RowData[]> {
  if (isPool(client)) {
    const connection = await client.acquire();
    try {
      return await executeOnClient(connection, query, params);
    } finally {
      await client.release(connection);
    }
  }

  const values =
    params.length > 0
      ? (params.map((param) => toNodeApiValue(param)) as DuckDBValue[])
      : undefined;
  const result = await client.run(query, values);
  const rows = await result.getRowsJS();
  const columns =
    // prefer deduplicated names when available (Node API >=1.4.2)
    result.deduplicatedColumnNames?.() ?? result.columnNames();
  const uniqueColumns = deduplicateColumns(columns);

  return rows ? mapRowsToObjects(uniqueColumns, rows) : [];
}

export interface ExecuteInBatchesOptions {
  rowsPerChunk?: number;
}

/**
 * Stream results from DuckDB in batches to avoid fully materializing rows in JS.
 */
export async function* executeInBatches(
  client: DuckDBClientLike,
  query: string,
  params: unknown[],
  options: ExecuteInBatchesOptions = {}
): AsyncGenerator<RowData[], void, void> {
  if (isPool(client)) {
    const connection = await client.acquire();
    try {
      yield* executeInBatches(connection, query, params, options);
      return;
    } finally {
      await client.release(connection);
    }
  }

  const rowsPerChunk =
    options.rowsPerChunk && options.rowsPerChunk > 0
      ? options.rowsPerChunk
      : 100_000;
  const values =
    params.length > 0
      ? (params.map((param) => toNodeApiValue(param)) as DuckDBValue[])
      : undefined;

  const result = await client.stream(query, values);
  const columns =
    // prefer deduplicated names when available (Node API >=1.4.2)
    result.deduplicatedColumnNames?.() ?? result.columnNames();
  const uniqueColumns = deduplicateColumns(columns);

  let buffer: RowData[] = [];

  for await (const chunk of result.yieldRowsJs()) {
    const objects = mapRowsToObjects(uniqueColumns, chunk);
    for (const row of objects) {
      buffer.push(row);
      if (buffer.length >= rowsPerChunk) {
        yield buffer;
        buffer = [];
      }
    }
  }

  if (buffer.length > 0) {
    yield buffer;
  }
}

/**
 * Return columnar results when the underlying node-api exposes an Arrow/columnar API.
 * Falls back to column-major JS arrays when Arrow is unavailable.
 */
export async function executeArrowOnClient(
  client: DuckDBClientLike,
  query: string,
  params: unknown[]
): Promise<unknown> {
  if (isPool(client)) {
    const connection = await client.acquire();
    try {
      return await executeArrowOnClient(connection, query, params);
    } finally {
      await client.release(connection);
    }
  }

  const values =
    params.length > 0
      ? (params.map((param) => toNodeApiValue(param)) as DuckDBValue[])
      : undefined;
  const result = await client.run(query, values);

  // Runtime detection for Arrow API support (optional method, not in base type)
  const maybeArrow =
    (result as unknown as { toArrow?: () => Promise<unknown> }).toArrow ??
    (result as unknown as { getArrowTable?: () => Promise<unknown> })
      .getArrowTable;

  if (typeof maybeArrow === 'function') {
    return await maybeArrow.call(result);
  }

  // Fallback: return column-major JS arrays to avoid per-row object creation.
  return result.getColumnsObjectJS();
}
