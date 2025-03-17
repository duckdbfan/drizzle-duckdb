/* Marked as internal in the original source, so we need to copy it here */
import {
  type SelectedFieldsOrdered,
  type AnyColumn,
  type DriverValueDecoder,
  is,
  Column,
  SQL,
  getTableName,
  sql,
} from 'drizzle-orm';
import { PgColumn, type SelectedFields } from 'drizzle-orm/pg-core';

// Need to get around "decoder" property being marked as internal
type SQLInternal<T = unknown> = SQL<T> & {
  decoder: DriverValueDecoder<T, any>;
};

export function mapResultRow<TResult>(
  columns: SelectedFieldsOrdered<AnyColumn>,
  row: unknown[],
  joinsNotNullableMap: Record<string, boolean> | undefined
): TResult {
  // Key -> nested object key, value -> table name if all fields in the nested object are from the same table, false otherwise
  const nullifyMap: Record<string, string | false> = {};

  const result = columns.reduce<Record<string, any>>(
    (result, { path, field }, columnIndex) => {
      let decoder: DriverValueDecoder<unknown, unknown>;
      if (is(field, Column)) {
        decoder = field;
      } else if (is(field, SQL)) {
        decoder = (field as SQLInternal).decoder;
      } else {
        decoder = (field.sql as SQLInternal).decoder;
      }
      let node = result;
      for (const [pathChunkIndex, pathChunk] of path.entries()) {
        if (pathChunkIndex < path.length - 1) {
          if (!(pathChunk in node)) {
            node[pathChunk] = {};
          }
          node = node[pathChunk];
          continue;
        }

        const rawValue = row[columnIndex]!;

        const value = (node[pathChunk] =
          rawValue === null ? null : decoder.mapFromDriverValue(rawValue));

        if (joinsNotNullableMap && is(field, Column) && path.length === 2) {
          const objectName = path[0]!;
          if (!(objectName in nullifyMap)) {
            nullifyMap[objectName] =
              value === null ? getTableName(field.table) : false;
          } else if (
            typeof nullifyMap[objectName] === 'string' &&
            nullifyMap[objectName] !== getTableName(field.table)
          ) {
            nullifyMap[objectName] = false;
          }
          continue;
        }

        // may need to add a condition for non-Aliased SQL
        if (
          joinsNotNullableMap &&
          is(field, SQL.Aliased) &&
          path.length === 2
        ) {
          const col = field.sql.queryChunks.find((chunk) => is(chunk, Column));
          const tableName = col?.table && getTableName(col?.table);

          if (!tableName) {
            continue;
          }

          const objectName = path[0]!;

          if (!(objectName in nullifyMap)) {
            nullifyMap[objectName] = value === null ? tableName : false;
            continue;
          }

          if (nullifyMap[objectName] && nullifyMap[objectName] !== tableName) {
            nullifyMap[objectName] = false;
          }
          continue;
        }
      }
      return result;
    },
    {}
  );

  // Nullify all nested objects from nullifyMap that are nullable
  if (joinsNotNullableMap && Object.keys(nullifyMap).length > 0) {
    for (const [objectName, tableName] of Object.entries(nullifyMap)) {
      if (typeof tableName === 'string' && !joinsNotNullableMap[tableName]) {
        result[objectName] = null;
      }
    }
  }

  return result as TResult;
}

export function aliasFields(
  fields: SelectedFields,
  fullJoin = false
): SelectedFields {
  return Object.fromEntries(
    Object.entries(fields).map(([key, value]) => {
      if (fullJoin && is(value, Column)) {
        return [
          key,
          sql`${value}`.as(`${getTableName(value.table)}.${value.name}`),
        ];
      }

      if (fullJoin && is(value, SQL)) {
        const col = value
          .getSQL()
          .queryChunks.find((chunk) => is(chunk, Column));

        const tableName = col?.table && getTableName(col?.table);

        return [key, value.as(tableName ? `${tableName}.${key}` : key)];
      }

      if (is(value, SQL) || is(value, Column)) {
        return [key, (is(value, SQL) ? value : sql`${value}`).as(key)];
      }

      if (is(value, SQL.Aliased)) {
        return [key, value];
      }

      // todo: should probably make this recursive?
      if (typeof value === 'object') {
        const parentKey = key;

        return [
          key,
          Object.fromEntries(
            Object.entries(value).map(([childKey, childValue]) => [
              childKey,
              (is(childValue, SQL) ? childValue : sql`${childValue}`).as(
                `${parentKey}.${childKey}`
              ),
            ])
          ),
        ];
      }

      return [key, value];
    })
  );
}

// DuckDB names returned variables differently than Postgres
// so we need to remap them to match the Postgres names

const selectionRegex = /select\s+(.+)\s+from/i;
// const tableIdPropSelectionRegex = /("(.+)"\."(.+)")(\s+as\s+'?(.+?)'?\.'?(.+?)'?)?/i;
const tableIdPropSelectionRegex = new RegExp(
  [
    `("(.+)"\\."(.+)")`, // table identifier + property
    `(\\s+as\\s+'?(.+?)'?\\.'?(.+?)'?)?`, // optional AS clause
  ].join(''),
  'i'
);
const noTableIdPropSelectionRegex = /"(.+)"(\s+as\s+'?\1'?)?/i;

const tablePropRegex = /"(.+)"\."(.+)"/i;
const asClauseRegex = /as\s+(.+)$/i;
const aliasRegex = /as\s+'?(.+)'?\.'?(.+)'?$/i;

/* Takes an SQL query as a string, and adds or updates "AS" clauses
 * to the form: `AS 'table_name.column_name'`
 * instead of : `AS "table_name"."column_name"`
 */
export function queryAdapter(query: string): string {
  // Things to consider:
  // - need to handle nested selects
  // - what about full joins?
  const selection = selectionRegex.exec(query);

  if (selection?.length !== 2) {
    return query;
  }

  const fields = selection[1].split(',').map((field) => {
    const trimmedField = field.trim();

    // - different scenarios:
    //    - no table identifier + no AS clause -> ignore
    //    - no table identifier + AS clause -> ensure AS clause format
    //    - table identifier + no AS clause -> add AS clause
    //    - table identifier + AS clause -> ensure AS clause format
    const propSelection = tableIdPropSelectionRegex
      .exec(trimmedField)
      ?.filter(Boolean);

    if (!propSelection) {
      return trimmedField;
    }
  });

  return query.replace(selection[1], fields.join(', '));
}
