/**
 * DuckDB-native array operators. Generate DuckDB-compatible SQL directly
 * without query rewriting.
 */

import { sql, type SQL, type SQLWrapper } from 'drizzle-orm';

export function arrayHasAll<T>(
  column: SQLWrapper,
  values: T[] | SQLWrapper
): SQL {
  return sql`array_has_all(${column}, ${values})`;
}

export function arrayHasAny<T>(
  column: SQLWrapper,
  values: T[] | SQLWrapper
): SQL {
  return sql`array_has_any(${column}, ${values})`;
}

export function arrayContainedBy<T>(
  column: SQLWrapper,
  values: T[] | SQLWrapper
): SQL {
  return sql`array_has_all(${values}, ${column})`;
}
