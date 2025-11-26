import {
  listValue,
  arrayValue,
  structValue,
  mapValue,
  blobValue,
  timestampValue,
  timestampTZValue,
  type DuckDBValue,
  type DuckDBMapEntry,
} from '@duckdb/node-api';

/**
 * Symbol used to identify wrapped DuckDB values for native binding.
 * Uses Symbol.for() to ensure cross-module compatibility.
 */
export const DUCKDB_VALUE_MARKER = Symbol.for('drizzle-duckdb:value');

/**
 * Type identifier for each wrapper kind.
 */
export type DuckDBValueKind =
  | 'list'
  | 'array'
  | 'struct'
  | 'map'
  | 'timestamp'
  | 'blob'
  | 'json';

/**
 * Base interface for all tagged DuckDB value wrappers.
 */
export interface DuckDBValueWrapper<
  TKind extends DuckDBValueKind = DuckDBValueKind,
  TData = unknown,
> {
  readonly [DUCKDB_VALUE_MARKER]: true;
  readonly kind: TKind;
  readonly data: TData;
}

/**
 * List wrapper - maps to DuckDBListValue
 */
export interface ListValueWrapper
  extends DuckDBValueWrapper<'list', unknown[]> {
  readonly elementType?: string;
}

/**
 * Array wrapper (fixed size) - maps to DuckDBArrayValue
 */
export interface ArrayValueWrapper
  extends DuckDBValueWrapper<'array', unknown[]> {
  readonly elementType?: string;
  readonly fixedLength?: number;
}

/**
 * Struct wrapper - maps to DuckDBStructValue
 */
export interface StructValueWrapper
  extends DuckDBValueWrapper<'struct', Record<string, unknown>> {
  readonly schema?: Record<string, string>;
}

/**
 * Map wrapper - maps to DuckDBMapValue
 */
export interface MapValueWrapper
  extends DuckDBValueWrapper<'map', Record<string, unknown>> {
  readonly valueType?: string;
}

/**
 * Timestamp wrapper - maps to DuckDBTimestampValue or DuckDBTimestampTZValue
 */
export interface TimestampValueWrapper
  extends DuckDBValueWrapper<'timestamp', Date | string> {
  readonly withTimezone: boolean;
  readonly precision?: number;
}

/**
 * Blob wrapper - maps to DuckDBBlobValue
 */
export interface BlobValueWrapper
  extends DuckDBValueWrapper<'blob', Buffer | Uint8Array> {}

/**
 * JSON wrapper - delays JSON.stringify() to binding time.
 * DuckDB stores JSON as VARCHAR internally.
 */
export interface JsonValueWrapper extends DuckDBValueWrapper<'json', unknown> {}

/**
 * Union of all wrapper types for exhaustive type checking.
 */
export type AnyDuckDBValueWrapper =
  | ListValueWrapper
  | ArrayValueWrapper
  | StructValueWrapper
  | MapValueWrapper
  | TimestampValueWrapper
  | BlobValueWrapper
  | JsonValueWrapper;

/**
 * Type guard to check if a value is a tagged DuckDB wrapper.
 * Optimized for fast detection in the hot path.
 */
export function isDuckDBWrapper(
  value: unknown
): value is AnyDuckDBValueWrapper {
  return (
    value !== null &&
    typeof value === 'object' &&
    DUCKDB_VALUE_MARKER in value &&
    (value as DuckDBValueWrapper)[DUCKDB_VALUE_MARKER] === true
  );
}

/**
 * Create a list wrapper for variable-length lists.
 */
export function wrapList(
  data: unknown[],
  elementType?: string
): ListValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'list',
    data,
    elementType,
  };
}

/**
 * Create an array wrapper for fixed-length arrays.
 */
export function wrapArray(
  data: unknown[],
  elementType?: string,
  fixedLength?: number
): ArrayValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'array',
    data,
    elementType,
    fixedLength,
  };
}

/**
 * Create a struct wrapper for named field structures.
 */
export function wrapStruct(
  data: Record<string, unknown>,
  schema?: Record<string, string>
): StructValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'struct',
    data,
    schema,
  };
}

/**
 * Create a map wrapper for key-value maps.
 */
export function wrapMap(
  data: Record<string, unknown>,
  valueType?: string
): MapValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'map',
    data,
    valueType,
  };
}

/**
 * Create a timestamp wrapper.
 */
export function wrapTimestamp(
  data: Date | string,
  withTimezone: boolean,
  precision?: number
): TimestampValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'timestamp',
    data,
    withTimezone,
    precision,
  };
}

/**
 * Create a blob wrapper for binary data.
 */
export function wrapBlob(data: Buffer | Uint8Array): BlobValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'blob',
    data,
  };
}

/**
 * Create a JSON wrapper that delays JSON.stringify() to binding time.
 * This ensures consistent handling with other wrapped types.
 */
export function wrapJson(data: unknown): JsonValueWrapper {
  return {
    [DUCKDB_VALUE_MARKER]: true,
    kind: 'json',
    data,
  };
}

/**
 * Convert a Date or string to microseconds since Unix epoch.
 * Handles both Date objects and ISO-like timestamp strings.
 */
function dateToMicros(value: Date | string): bigint {
  if (value instanceof Date) {
    return BigInt(value.getTime()) * 1000n;
  }

  // For strings, normalize the format for reliable parsing
  // Handle both 'YYYY-MM-DD HH:MM:SS' and 'YYYY-MM-DDTHH:MM:SS' formats
  let normalized = value;
  if (!value.includes('T') && value.includes(' ')) {
    // Convert 'YYYY-MM-DD HH:MM:SS' to ISO format
    normalized = value.replace(' ', 'T');
  }
  // Add 'Z' suffix if no timezone offset to treat as UTC
  if (!normalized.endsWith('Z') && !/[+-]\d{2}:?\d{2}$/.test(normalized)) {
    normalized += 'Z';
  }

  const date = new Date(normalized);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid timestamp string: ${value}`);
  }
  return BigInt(date.getTime()) * 1000n;
}

/**
 * Convert Buffer or Uint8Array to Uint8Array.
 */
function toUint8Array(data: Buffer | Uint8Array): Uint8Array {
  return data instanceof Uint8Array && !(data instanceof Buffer)
    ? data
    : new Uint8Array(data);
}

/**
 * Convert struct entries to DuckDB struct value entries.
 */
function convertStructEntries(
  data: Record<string, unknown>,
  toValue: (v: unknown) => DuckDBValue
): Record<string, DuckDBValue> {
  const entries: Record<string, DuckDBValue> = {};
  for (const [key, val] of Object.entries(data)) {
    entries[key] = toValue(val);
  }
  return entries;
}

/**
 * Convert map entries to DuckDB map entry format.
 */
function convertMapEntries(
  data: Record<string, unknown>,
  toValue: (v: unknown) => DuckDBValue
): DuckDBMapEntry[] {
  return Object.entries(data).map(([key, val]) => ({
    key: key as DuckDBValue,
    value: toValue(val),
  }));
}

/**
 * Convert a wrapper to a DuckDB Node API value.
 * Uses exhaustive switch for compile-time safety.
 */
export function wrapperToNodeApiValue(
  wrapper: AnyDuckDBValueWrapper,
  toValue: (v: unknown) => DuckDBValue
): DuckDBValue {
  switch (wrapper.kind) {
    case 'list':
      return listValue(wrapper.data.map(toValue));
    case 'array':
      return arrayValue(wrapper.data.map(toValue));
    case 'struct':
      return structValue(convertStructEntries(wrapper.data, toValue));
    case 'map':
      return mapValue(convertMapEntries(wrapper.data, toValue));
    case 'timestamp':
      return wrapper.withTimezone
        ? timestampTZValue(dateToMicros(wrapper.data))
        : timestampValue(dateToMicros(wrapper.data));
    case 'blob':
      return blobValue(toUint8Array(wrapper.data));
    case 'json':
      // JSON is stored as VARCHAR in DuckDB - stringify at binding time
      return JSON.stringify(wrapper.data);
    default: {
      // Exhaustive check - TypeScript will error if a case is missing
      const _exhaustive: never = wrapper;
      throw new Error(
        `Unknown wrapper kind: ${(_exhaustive as AnyDuckDBValueWrapper).kind}`
      );
    }
  }
}
