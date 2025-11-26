import {
  listValue,
  type DuckDBConnection,
  type DuckDBValue,
} from '@duckdb/node-api';

export type DuckDBClientLike = DuckDBConnection;
export type RowData = Record<string, unknown>;

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

let warnedArrayLiteral = false;

export function prepareParams(
  params: unknown[],
  options: PrepareParamsOptions = {}
): unknown[] {
  return params.map((param) => {
    if (typeof param === 'string' && isPgArrayLiteral(param)) {
      if (options.rejectStringArrayLiterals) {
        throw new Error(
          'Stringified array literals are not supported. Use duckDbList()/duckDbArray() or pass native arrays.'
        );
      }

      if (!warnedArrayLiteral && options.warnOnStringArrayLiteral) {
        warnedArrayLiteral = true;
        options.warnOnStringArrayLiteral();
      }
      return parsePgArrayLiteral(param);
    }
    return param;
  });
}

function toNodeApiValue(value: unknown): DuckDBValue {
  if (Array.isArray(value)) {
    return listValue(value.map((inner) => toNodeApiValue(inner)));
  }
  return value as DuckDBValue;
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
  const values =
    params.length > 0
      ? (params.map((param) => toNodeApiValue(param)) as DuckDBValue[])
      : undefined;
  const result = await client.run(query, values);
  const rows = await result.getRowsJS();
  const columns = result.columnNames();
  const seen: Record<string, number> = {};
  const uniqueColumns = columns.map((col) => {
    const count = seen[col] ?? 0;
    seen[col] = count + 1;
    return count === 0 ? col : `${col}_${count}`;
  });

  return (rows ?? []).map((vals) => {
    const obj: Record<string, unknown> = {};
    uniqueColumns.forEach((col, idx) => {
      obj[col] = vals[idx];
    });
    return obj;
  }) as RowData[];
}
