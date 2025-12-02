/**
 * AST visitor to qualify unqualified column references in JOIN ON clauses.
 */

import type {
  AST,
  Binary,
  ColumnRefItem,
  ExpressionValue,
  Select,
  From,
  Join,
  OrderBy,
  Column,
} from 'node-sql-parser';

type TableSource = {
  name: string;
  alias: string | null;
};

function getTableSource(from: From): TableSource | null {
  if ('table' in from && from.table) {
    return {
      name: from.table,
      alias: from.as ?? null,
    };
  }
  if ('expr' in from && from.as) {
    return {
      name: from.as,
      alias: from.as,
    };
  }
  return null;
}

function getQualifier(source: TableSource): string {
  return source.alias ?? source.name;
}

function isUnqualifiedColumnRef(expr: ExpressionValue): expr is ColumnRefItem {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    'type' in expr &&
    expr.type === 'column_ref' &&
    !('table' in expr && expr.table)
  );
}

function getColumnName(col: ColumnRefItem): string | null {
  if (typeof col.column === 'string') {
    return col.column;
  }
  if (col.column && 'expr' in col.column && col.column.expr?.value) {
    return String(col.column.expr.value);
  }
  return null;
}

function walkOnClause(
  expr: Binary | null | undefined,
  leftSource: string,
  rightSource: string,
  ambiguousColumns: Set<string>
): boolean {
  if (!expr || typeof expr !== 'object') return false;

  let transformed = false;

  if (expr.type === 'binary_expr') {
    if (expr.operator === '=') {
      const left = expr.left as ExpressionValue;
      const right = expr.right as ExpressionValue;

      if (isUnqualifiedColumnRef(left) && isUnqualifiedColumnRef(right)) {
        const leftColName = getColumnName(left);
        const rightColName = getColumnName(right);

        if (leftColName && rightColName && leftColName === rightColName) {
          left.table = leftSource;
          right.table = rightSource;

          ambiguousColumns.add(leftColName);
          transformed = true;
        }
      }
    }

    if (expr.operator === 'AND' || expr.operator === 'OR') {
      transformed =
        walkOnClause(
          expr.left as Binary,
          leftSource,
          rightSource,
          ambiguousColumns
        ) || transformed;
      transformed =
        walkOnClause(
          expr.right as Binary,
          leftSource,
          rightSource,
          ambiguousColumns
        ) || transformed;
    }
  }

  return transformed;
}

function qualifyAmbiguousInExpression(
  expr: ExpressionValue | null | undefined,
  defaultQualifier: string,
  ambiguousColumns: Set<string>
): boolean {
  if (!expr || typeof expr !== 'object') return false;

  let transformed = false;

  if (isUnqualifiedColumnRef(expr)) {
    const colName = getColumnName(expr);
    if (colName && ambiguousColumns.has(colName)) {
      expr.table = defaultQualifier;
      transformed = true;
    }
    return transformed;
  }

  if ('type' in expr && expr.type === 'binary_expr') {
    const binary = expr as Binary;
    transformed =
      qualifyAmbiguousInExpression(
        binary.left as ExpressionValue,
        defaultQualifier,
        ambiguousColumns
      ) || transformed;
    transformed =
      qualifyAmbiguousInExpression(
        binary.right as ExpressionValue,
        defaultQualifier,
        ambiguousColumns
      ) || transformed;
    return transformed;
  }

  if ('args' in expr && expr.args) {
    const args = expr.args as {
      value?: ExpressionValue[];
      expr?: ExpressionValue;
    };
    if (args.value && Array.isArray(args.value)) {
      for (const arg of args.value) {
        transformed =
          qualifyAmbiguousInExpression(
            arg,
            defaultQualifier,
            ambiguousColumns
          ) || transformed;
      }
    }
    if (args.expr) {
      transformed =
        qualifyAmbiguousInExpression(
          args.expr,
          defaultQualifier,
          ambiguousColumns
        ) || transformed;
    }
  }

  return transformed;
}

function walkSelect(select: Select): boolean {
  let transformed = false;
  const ambiguousColumns = new Set<string>();

  if (Array.isArray(select.from) && select.from.length >= 2) {
    const firstSource = getTableSource(select.from[0]);
    const defaultQualifier = firstSource ? getQualifier(firstSource) : '';
    let prevSource = firstSource;

    for (const from of select.from) {
      if ('join' in from) {
        const join = from as Join;
        const currentSource = getTableSource(join);

        if (join.on && prevSource && currentSource) {
          const leftQualifier = getQualifier(prevSource);
          const rightQualifier = getQualifier(currentSource);

          transformed =
            walkOnClause(
              join.on,
              leftQualifier,
              rightQualifier,
              ambiguousColumns
            ) || transformed;
        }

        prevSource = currentSource;
      } else {
        const source = getTableSource(from);
        if (source) {
          prevSource = source;
        }
      }

      if ('expr' in from && from.expr && 'ast' in from.expr) {
        transformed = walkSelect(from.expr.ast) || transformed;
      }
    }

    if (ambiguousColumns.size > 0 && defaultQualifier) {
      if (Array.isArray(select.columns)) {
        for (const col of select.columns as Column[]) {
          if ('expr' in col) {
            transformed =
              qualifyAmbiguousInExpression(
                col.expr,
                defaultQualifier,
                ambiguousColumns
              ) || transformed;
          }
        }
      }

      transformed =
        qualifyAmbiguousInExpression(
          select.where,
          defaultQualifier,
          ambiguousColumns
        ) || transformed;

      if (Array.isArray(select.orderby)) {
        for (const order of select.orderby as OrderBy[]) {
          if (order.expr) {
            transformed =
              qualifyAmbiguousInExpression(
                order.expr,
                defaultQualifier,
                ambiguousColumns
              ) || transformed;
          }
        }
      }
    }
  }

  if (select.with) {
    for (const cte of select.with) {
      const cteSelect = cte.stmt?.ast ?? cte.stmt;
      if (cteSelect && cteSelect.type === 'select') {
        transformed = walkSelect(cteSelect as Select) || transformed;
      }
    }
  }

  if (select._next) {
    transformed = walkSelect(select._next) || transformed;
  }

  return transformed;
}

export function qualifyJoinColumns(ast: AST | AST[]): boolean {
  const statements = Array.isArray(ast) ? ast : [ast];
  let transformed = false;

  for (const stmt of statements) {
    if (stmt.type === 'select') {
      transformed = walkSelect(stmt as Select) || transformed;
    }
  }

  return transformed;
}
