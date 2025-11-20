import type { Connection, Database, RowData } from 'duckdb-async';
import {
  listValue,
  type DuckDBConnection as NodeApiDuckDBConnection,
  type DuckDBValue as NodeApiDuckDBValue,
} from '@duckdb/node-api';

export type DuckDBClient = Database;
export type DuckDBClientLike =
  | DuckDBClient
  | Connection
  | NodeApiDuckDBConnection;

function isDuckDbAsyncClient(
  client: DuckDBClientLike
): client is DuckDBClient | Connection {
  return typeof (client as Connection).all === 'function';
}

function isNodeApiClient(
  client: DuckDBClientLike
): client is NodeApiDuckDBConnection {
  return typeof (client as NodeApiDuckDBConnection).run === 'function';
}

export function hasConnect(client: DuckDBClientLike): client is DuckDBClient {
  return typeof (client as DuckDBClient).connect === 'function';
}

function parsePgArrayLiteral(value: string): unknown {
  if (!value.startsWith('{') || !value.endsWith('}')) {
    return value;
  }

  const json = value.replace(/{/g, '[').replace(/}/g, ']');

  try {
    return JSON.parse(json);
  } catch {
    return value;
  }
}

export function prepareParams(params: unknown[]): unknown[] {
  return params.map((param) => {
    if (typeof param === 'string') {
      return parsePgArrayLiteral(param);
    }
    return param;
  });
}

function toNodeApiValue(value: unknown): NodeApiDuckDBValue {
  if (Array.isArray(value)) {
    return listValue(value.map((inner) => toNodeApiValue(inner)));
  }
  return value as NodeApiDuckDBValue;
}

export async function closeClientConnection(
  connection: Connection | NodeApiDuckDBConnection
): Promise<void> {
  if ('close' in connection && typeof connection.close === 'function') {
    await connection.close();
    return;
  }

  if (
    'closeSync' in connection &&
    typeof connection.closeSync === 'function'
  ) {
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
  if (isDuckDbAsyncClient(client)) {
    return (await client.all(query, ...params)) ?? [];
  }

  if (isNodeApiClient(client)) {
    const values =
      params.length > 0
        ? (params.map((param) => toNodeApiValue(param)) as NodeApiDuckDBValue[])
        : undefined;
    const result = await client.run(query, values);
    const rows = await result.getRowsJS();
    const columns = result.columnNames();

    return (rows ?? []).map((vals) => {
      const obj: Record<string, unknown> = {};
      columns.forEach((col, idx) => {
        obj[col] = vals[idx];
      });
      return obj;
    }) as RowData[];
  }

  throw new Error(
    'Unsupported DuckDB client: expected duckdb-async Database/Connection or @duckdb/node-api Connection.'
  );
}
