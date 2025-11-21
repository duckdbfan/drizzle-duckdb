import {
  Column,
  SQL,
  getTableName,
  is,
  sql,
} from 'drizzle-orm';
import type { SelectedFields } from 'drizzle-orm/pg-core';

function mapEntries(
  obj: Record<string, unknown>,
  prefix?: string,
  fullJoin = false
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(obj)
      .filter(([key]) => key !== 'enableRLS')
      .map(([key, value]) => {
        const qualified = prefix ? `${prefix}.${key}` : key;

        if (fullJoin && is(value, Column)) {
          return [
            key,
            sql`${value}`
              .mapWith(value)
              .as(`${getTableName(value.table)}.${value.name}`),
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
          const aliased =
            is(value, SQL) ? value : sql`${value}`.mapWith(value);
          return [key, aliased.as(qualified)];
        }

        if (is(value, SQL.Aliased)) {
          return [key, value];
        }

        if (typeof value === 'object' && value !== null) {
          return [
            key,
            mapEntries(value as Record<string, unknown>, qualified, fullJoin),
          ];
        }

        return [key, value];
      })
  );
}

export function aliasFields(
  fields: SelectedFields,
  fullJoin = false
): SelectedFields {
  return mapEntries(fields, undefined, fullJoin) as SelectedFields;
}
