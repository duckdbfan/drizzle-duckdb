/**
 * AST-based SQL transformer for DuckDB compatibility.
 *
 * Transforms:
 * - Array operators: @>, <@, && -> array_has_all(), array_has_any()
 * - JOIN column qualification: "col" = "col" -> "left"."col" = "right"."col"
 */

import nodeSqlParser from 'node-sql-parser';
const { Parser } = nodeSqlParser;
import type { AST } from 'node-sql-parser';

import { transformArrayOperators } from './visitors/array-operators.ts';
import { qualifyJoinColumns } from './visitors/column-qualifier.ts';

const parser = new Parser();

export type TransformResult = {
  sql: string;
  transformed: boolean;
};

export function transformSQL(query: string): TransformResult {
  const needsArrayTransform =
    query.includes('@>') || query.includes('<@') || query.includes('&&');
  const needsJoinTransform = query.toLowerCase().includes('join');

  if (!needsArrayTransform && !needsJoinTransform) {
    return { sql: query, transformed: false };
  }

  try {
    const ast = parser.astify(query, { database: 'PostgreSQL' });

    let transformed = false;

    if (needsArrayTransform) {
      transformed = transformArrayOperators(ast) || transformed;
    }

    if (needsJoinTransform) {
      transformed = qualifyJoinColumns(ast) || transformed;
    }

    if (!transformed) {
      return { sql: query, transformed: false };
    }

    const transformedSql = parser.sqlify(ast, { database: 'PostgreSQL' });

    return { sql: transformedSql, transformed: true };
  } catch {
    return { sql: query, transformed: false };
  }
}

export function needsTransformation(query: string): boolean {
  const lower = query.toLowerCase();
  return (
    query.includes('@>') ||
    query.includes('<@') ||
    query.includes('&&') ||
    lower.includes('join')
  );
}

export { transformArrayOperators } from './visitors/array-operators.ts';
export { qualifyJoinColumns } from './visitors/column-qualifier.ts';
