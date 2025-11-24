import { sql, type SQL } from 'drizzle-orm';
import type { SQLWrapper } from 'drizzle-orm/sql/sql';
import { customType } from 'drizzle-orm/pg-core';

type IntColType =
  | 'SMALLINT'
  | 'INTEGER'
  | 'BIGINT'
  | 'HUGEINT'
  | 'USMALLINT'
  | 'UINTEGER'
  | 'UBIGINT'
  | 'UHUGEINT'
  | 'INT'
  | 'INT16'
  | 'INT32'
  | 'INT64'
  | 'INT128'
  | 'LONG'
  | 'VARINT';

type FloatColType = 'FLOAT' | 'DOUBLE';

type StringColType = 'STRING' | 'VARCHAR' | 'TEXT';

type BoolColType = 'BOOLEAN' | 'BOOL';

type BlobColType = 'BLOB' | 'BYTEA' | 'VARBINARY';

type DateColType =
  | 'DATE'
  | 'TIME'
  | 'TIMETZ'
  | 'TIMESTAMP'
  | 'DATETIME'
  | 'TIMESTAMPTZ'
  | 'TIMESTAMP_MS'
  | 'TIMESTAMP_S';

type AnyColType =
  | IntColType
  | FloatColType
  | StringColType
  | BoolColType
  | DateColType
  | BlobColType;

type ListColType = `${AnyColType}[]`;
type ArrayColType = `${AnyColType}[${number}]`;
type StructColType = `STRUCT (${string})`;

type Primitive = AnyColType | ListColType | ArrayColType | StructColType;

function coerceArrayString(value: string): unknown[] | undefined {
  const trimmed = value.trim();
  if (!trimmed) {
    return [];
  }
  if (trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed) as unknown[];
    } catch {
      return undefined;
    }
  }
  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    try {
      const json = trimmed.replace(/{/g, '[').replace(/}/g, ']');
      return JSON.parse(json) as unknown[];
    } catch {
      return undefined;
    }
  }
  return undefined;
}

function formatLiteral(value: unknown, typeHint?: string): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  const upperType = typeHint?.toUpperCase() ?? '';
  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return value.toString();
  }

  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }

  const str =
    typeof value === 'string' ? value : JSON.stringify(value) ?? String(value);

  const escaped = str.replace(/'/g, "''");
  // Simple quoting based on hint.
  if (
    upperType.includes('CHAR') ||
    upperType.includes('TEXT') ||
    upperType.includes('STRING') ||
    upperType.includes('VARCHAR')
  ) {
    return `'${escaped}'`;
  }

  return `'${escaped}'`;
}

function buildListLiteral(values: unknown[], elementType?: string): SQL {
  if (values.length === 0) {
    return sql`[]`;
  }
  const chunks = values.map((v) =>
    typeof v === 'object' && !Array.isArray(v)
      ? sql`${v as SQLWrapper}`
      : sql.raw(formatLiteral(v, elementType))
  );
  return sql`list_value(${sql.join(chunks, sql.raw(', '))})`;
}

function buildStructLiteral(
  value: Record<string, unknown>,
  schema?: Record<string, Primitive>
): SQL {
  const parts = Object.entries(value).map(([key, val]) => {
    const typeHint = schema?.[key];
    if (Array.isArray(val)) {
      const inner =
        typeof typeHint === 'string' && typeHint.endsWith('[]')
          ? typeHint.slice(0, -2)
          : undefined;

      return sql`${sql.identifier(key)} := ${buildListLiteral(val, inner)}`;
    }
    return sql`${sql.identifier(key)} := ${val}`;
  });
  return sql`struct_pack(${sql.join(parts, sql.raw(', '))})`;
}

function buildMapLiteral(value: Record<string, unknown>, valueType?: string): SQL {
  const keys = Object.keys(value);
  const vals = Object.values(value);
  const keyList = buildListLiteral(keys, 'TEXT');
  const valList = buildListLiteral(
    vals,
    valueType?.endsWith('[]') ? valueType.slice(0, -2) : valueType
  );
  return sql`map(${keyList}, ${valList})`;
}

export const duckDbList = <TData = unknown>(
  name: string,
  elementType: AnyColType
) =>
  customType<{ data: TData[]; driverData: SQL | unknown[] | string }>({
    dataType() {
      return `${elementType}[]`;
    },
    toDriver(value: TData[]) {
      return buildListLiteral(value, elementType);
    },
    fromDriver(value: unknown[] | string | SQL): TData[] {
      if (Array.isArray(value)) {
        return value as TData[];
      }
      if (typeof value === 'string') {
        const parsed = coerceArrayString(value);
        if (parsed) {
          return parsed as TData[];
        }
      }
      return [] as TData[];
    },
  })(name);

export const duckDbArray = <TData = unknown>(
  name: string,
  elementType: AnyColType,
  fixedLength?: number
) =>
  customType<{ data: TData[]; driverData: SQL | unknown[] | string }>({
    dataType() {
      return fixedLength
        ? `${elementType}[${fixedLength}]`
        : `${elementType}[]`;
    },
    toDriver(value: TData[]) {
      return buildListLiteral(value, elementType);
    },
    fromDriver(value: unknown[] | string | SQL): TData[] {
      if (Array.isArray(value)) {
        return value as TData[];
      }
      if (typeof value === 'string') {
        const parsed = coerceArrayString(value);
        if (parsed) {
          return parsed as TData[];
        }
      }
      return [] as TData[];
    },
  })(name);

export const duckDbMap = <TData extends Record<string, any>>(
  name: string,
  valueType: AnyColType | ListColType | ArrayColType
) =>
  customType<{ data: TData; driverData: TData }>({
  dataType() {
      return `MAP (STRING, ${valueType})`;
    },
    toDriver(value: TData) {
      return buildMapLiteral(value, valueType);
    },
    fromDriver(value: TData): TData {
      return value;
    },
  })(name);

export const duckDbStruct = <TData extends Record<string, any>>(
  name: string,
  schema: Record<string, Primitive>
) =>
  customType<{ data: TData; driverData: TData }>({
    dataType() {
      const fields = Object.entries(schema).map(
        ([key, type]) => `${key} ${type}`
      );

      return `STRUCT (${fields.join(', ')})`;
    },
    toDriver(value: TData) {
      return buildStructLiteral(value, schema);
    },
    fromDriver(value: TData | string): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value;
    },
  })(name);

export const duckDbJson = <TData = unknown>(name: string) =>
  customType<{ data: TData; driverData: SQL | string }>({
    dataType() {
      return 'JSON';
    },
    toDriver(value: TData) {
      if (typeof value === 'string') {
        return value;
      }
      if (
        value !== null &&
        typeof value === 'object' &&
        'queryChunks' in (value as Record<string, unknown>)
      ) {
        return value as unknown as SQL;
      }
      return JSON.stringify(value ?? null);
    },
    fromDriver(value: SQL | string) {
      if (typeof value !== 'string') {
        return value as unknown as TData;
      }
      const trimmed = value.trim();
      if (!trimmed) {
        return value as unknown as TData;
      }
      try {
        return JSON.parse(trimmed) as TData;
      } catch {
        return value as unknown as TData;
      }
    },
  })(name);

export const duckDbBlob = customType<{
  data: Buffer;
  default: false;
}>({
  dataType() {
    return 'BLOB';
  },
  toDriver(value: Buffer) {
    const hexString = value.toString('hex');
    return sql`from_hex(${hexString})`;
  },
});

export const duckDbInet = (name: string) =>
  customType<{ data: string; driverData: string }>({
    dataType() {
      return 'INET';
    },
    toDriver(value: string) {
      return value;
    },
  })(name);

export const duckDbInterval = (name: string) =>
  customType<{ data: string; driverData: string }>({
    dataType() {
      return 'INTERVAL';
    },
    toDriver(value: string) {
      return value;
    },
  })(name);

type TimestampMode = 'date' | 'string';

interface TimestampOptions {
  withTimezone?: boolean;
  mode?: TimestampMode;
  precision?: number;
}

export const duckDbTimestamp = (
  name: string,
  options: TimestampOptions = {}
) =>
  customType<{
    data: Date | string;
    driverData: SQL | string | Date;
  }>({
    dataType() {
      if (options.withTimezone) {
        return 'TIMESTAMPTZ';
      }
      const precision = options.precision ? `(${options.precision})` : '';
      return `TIMESTAMP${precision}`;
    },
    toDriver(value: Date | string) {
      const iso = value instanceof Date ? value.toISOString() : value;
      const normalized = iso.replace('T', ' ').replace('Z', '+00');
      const typeKeyword = options.withTimezone ? 'TIMESTAMPTZ' : 'TIMESTAMP';
      return sql.raw(`${typeKeyword} '${normalized}'`);
    },
    fromDriver(value: Date | string | SQL) {
      if (options.mode === 'string') {
        if (value instanceof Date) {
          return value.toISOString().replace('T', ' ').replace('Z', '+00');
        }
        return typeof value === 'string' ? value : value.toString();
      }
      if (value instanceof Date) {
        return value;
      }
      const stringValue =
        typeof value === 'string' ? value : value.toString();
      const hasOffset =
        stringValue.endsWith('Z') ||
        /[+-]\d{2}:?\d{2}$/.test(stringValue);
      const normalized = hasOffset
        ? stringValue.replace(' ', 'T')
        : `${stringValue.replace(' ', 'T')}Z`;
      return new Date(normalized);
    },
  })(name);

export const duckDbDate = (name: string) =>
  customType<{ data: string | Date; driverData: string | Date }>({
    dataType() {
      return 'DATE';
    },
    toDriver(value: string | Date) {
      return value;
    },
    fromDriver(value: string | Date) {
        const str =
          value instanceof Date
            ? value.toISOString().slice(0, 10)
            : value;
        return str;
    },
  })(name);

export const duckDbTime = (name: string) =>
  customType<{ data: string; driverData: string | bigint }>({
    dataType() {
      return 'TIME';
    },
    toDriver(value: string) {
      return value;
    },
    fromDriver(value: string | bigint) {
      if (typeof value === 'bigint') {
        const totalMillis = Number(value) / 1000;
        const date = new Date(totalMillis);
        return date.toISOString().split('T')[1]!.replace('Z', '');
      }
      return value;
    },
  })(name);

function toListValue(values: (unknown | SQLWrapper)[]): SQL {
  return buildListLiteral(values);
}

export function duckDbArrayContains(
  column: SQLWrapper,
  values: unknown[] | SQLWrapper
): SQL {
  const rhs = Array.isArray(values) ? toListValue(values) : values;
  return sql`array_has_all(${column}, ${rhs})`;
}

export function duckDbArrayContained(
  column: SQLWrapper,
  values: unknown[] | SQLWrapper
): SQL {
  const rhs = Array.isArray(values) ? toListValue(values) : values;
  return sql`array_has_all(${rhs}, ${column})`;
}

export function duckDbArrayOverlaps(
  column: SQLWrapper,
  values: unknown[] | SQLWrapper
): SQL {
  const rhs = Array.isArray(values) ? toListValue(values) : values;
  return sql`array_has_any(${column}, ${rhs})`;
}
