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
import {
  DUCKDB_VALUE_MARKER,
  isDuckDBWrapper,
  wrapArray,
  wrapBlob,
  wrapJson,
  wrapList,
  wrapMap,
  wrapStruct,
  wrapTimestamp,
  type AnyDuckDBValueWrapper,
  type DuckDBValueWrapper,
  type ArrayValueWrapper,
  type BlobValueWrapper,
  type JsonValueWrapper,
  type ListValueWrapper,
  type MapValueWrapper,
  type StructValueWrapper,
  type TimestampValueWrapper,
  type DuckDBValueKind,
} from './value-wrappers-core.ts';

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

// Re-export core helpers for convenience and backward compatibility.
export {
  DUCKDB_VALUE_MARKER,
  isDuckDBWrapper,
  wrapArray,
  wrapBlob,
  wrapJson,
  wrapList,
  wrapMap,
  wrapStruct,
  wrapTimestamp,
  type AnyDuckDBValueWrapper,
  type DuckDBValueWrapper,
  type ArrayValueWrapper,
  type BlobValueWrapper,
  type JsonValueWrapper,
  type ListValueWrapper,
  type MapValueWrapper,
  type StructValueWrapper,
  type TimestampValueWrapper,
  type DuckDBValueKind,
} from './value-wrappers-core.ts';
