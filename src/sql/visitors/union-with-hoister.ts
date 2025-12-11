/**
 * AST visitor to hoist WITH clauses out of UNION and other set operations.
 *
 * Drizzle can emit SQL like:
 *   (with a as (...) select ...) union (with b as (...) select ...)
 *
 * DuckDB 1.4.x has an internal binder bug for this pattern.
 * We merge per arm CTEs into a single top level WITH when names do not collide.
 */

import type { AST, Select, From } from 'node-sql-parser';

function getCteName(cte: { name?: unknown }): string | null {
  const nameObj = cte.name as Record<string, unknown> | undefined;
  if (!nameObj) return null;
  const value = nameObj.value;
  if (typeof value === 'string') return value;
  return null;
}

function hoistWithInSelect(select: Select): boolean {
  if (!select.set_op || !select._next) return false;

  const arms: Select[] = [];
  let current: Select | null = select;
  while (current && current.type === 'select') {
    arms.push(current);
    current = current._next as Select | null;
  }

  const mergedWith: NonNullable<Select['with']> = [];
  const seen = new Set<string>();
  let hasWithBeyondFirst = false;

  for (const arm of arms) {
    if (arm.with && arm.with.length > 0) {
      if (arm !== arms[0]) {
        hasWithBeyondFirst = true;
      }
      for (const cte of arm.with) {
        const cteName = getCteName(cte);
        if (!cteName) return false;
        if (seen.has(cteName)) {
          return false;
        }
        seen.add(cteName);
        mergedWith.push(cte);
      }
    }
  }

  if (!hasWithBeyondFirst) return false;

  arms[0].with = mergedWith;
  if ('parentheses_symbol' in arms[0]) {
    (arms[0] as Select & { parentheses_symbol?: boolean }).parentheses_symbol =
      false;
  }
  for (let i = 1; i < arms.length; i++) {
    arms[i].with = null;
  }

  return true;
}

function walkSelect(select: Select): boolean {
  let transformed = false;

  if (select.with) {
    for (const cte of select.with) {
      const cteSelect = cte.stmt?.ast ?? cte.stmt;
      if (cteSelect && cteSelect.type === 'select') {
        transformed = walkSelect(cteSelect as Select) || transformed;
      }
    }
  }

  if (Array.isArray(select.from)) {
    for (const from of select.from as From[]) {
      if ('expr' in from && from.expr && 'ast' in from.expr) {
        transformed = walkSelect(from.expr.ast as Select) || transformed;
      }
    }
  }

  transformed = hoistWithInSelect(select) || transformed;

  if (select._next) {
    transformed = walkSelect(select._next) || transformed;
  }

  return transformed;
}

export function hoistUnionWith(ast: AST | AST[]): boolean {
  const statements = Array.isArray(ast) ? ast : [ast];
  let transformed = false;

  for (const stmt of statements) {
    if (stmt.type === 'select') {
      transformed = walkSelect(stmt as Select) || transformed;
    }
  }

  return transformed;
}
