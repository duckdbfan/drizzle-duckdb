type ArrayOperator = {
  token: '@>' | '<@' | '&&';
  fn: 'array_has_all' | 'array_has_any';
  swap?: boolean;
};

const OPERATORS: ArrayOperator[] = [
  { token: '@>', fn: 'array_has_all' },
  { token: '<@', fn: 'array_has_all', swap: true },
  { token: '&&', fn: 'array_has_any' },
];

const isWhitespace = (char: string | undefined) =>
  char !== undefined && /\s/.test(char);

export function scrubForRewrite(query: string): string {
  let scrubbed = '';
  type State = 'code' | 'single' | 'double' | 'lineComment' | 'blockComment';
  let state: State = 'code';

  for (let i = 0; i < query.length; i += 1) {
    const char = query[i]!;
    const next = query[i + 1];

    if (state === 'code') {
      if (char === "'") {
        scrubbed += "'";
        state = 'single';
        continue;
      }
      if (char === '"') {
        scrubbed += '"';
        state = 'double';
        continue;
      }
      if (char === '-' && next === '-') {
        scrubbed += '  ';
        i += 1;
        state = 'lineComment';
        continue;
      }
      if (char === '/' && next === '*') {
        scrubbed += '  ';
        i += 1;
        state = 'blockComment';
        continue;
      }

      scrubbed += char;
      continue;
    }

    if (state === 'single') {
      if (char === "'" && next === "'") {
        scrubbed += "''";
        i += 1;
        continue;
      }
      // Preserve quote for boundary detection but mask inner chars with a
      // non-whitespace placeholder to avoid false positives on operators.
      scrubbed += char === "'" ? "'" : '.';
      if (char === "'") {
        state = 'code';
      }
      continue;
    }

    if (state === 'double') {
      if (char === '"' && next === '"') {
        scrubbed += '""';
        i += 1;
        continue;
      }
      scrubbed += char === '"' ? '"' : '.';
      if (char === '"') {
        state = 'code';
      }
      continue;
    }

    if (state === 'lineComment') {
      scrubbed += char === '\n' ? '\n' : ' ';
      if (char === '\n') {
        state = 'code';
      }
      continue;
    }

    if (state === 'blockComment') {
      if (char === '*' && next === '/') {
        scrubbed += '  ';
        i += 1;
        state = 'code';
      } else {
        scrubbed += ' ';
      }
    }
  }

  return scrubbed;
}

function findNextOperator(
  scrubbed: string,
  start: number
): { index: number; operator: ArrayOperator } | null {
  for (let idx = start; idx < scrubbed.length; idx += 1) {
    for (const operator of OPERATORS) {
      if (scrubbed.startsWith(operator.token, idx)) {
        return { index: idx, operator };
      }
    }
  }
  return null;
}

function walkLeft(
  source: string,
  scrubbed: string,
  start: number
): [number, string] {
  let idx = start;
  while (idx >= 0 && isWhitespace(scrubbed[idx])) {
    idx -= 1;
  }

  let depth = 0;
  for (; idx >= 0; idx -= 1) {
    const ch = scrubbed[idx];
    if (ch === ')' || ch === ']') {
      depth += 1;
    } else if (ch === '(' || ch === '[') {
      if (depth === 0) {
        return [idx + 1, source.slice(idx + 1, start + 1)];
      }
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && isWhitespace(ch)) {
      return [idx + 1, source.slice(idx + 1, start + 1)];
    }
  }

  return [0, source.slice(0, start + 1)];
}

function walkRight(
  source: string,
  scrubbed: string,
  start: number
): [number, string] {
  let idx = start;
  while (idx < scrubbed.length && isWhitespace(scrubbed[idx])) {
    idx += 1;
  }

  let depth = 0;
  for (; idx < scrubbed.length; idx += 1) {
    const ch = scrubbed[idx];
    if (ch === '(' || ch === '[') {
      depth += 1;
    } else if (ch === ')' || ch === ']') {
      if (depth === 0) {
        return [idx, source.slice(start, idx)];
      }
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && isWhitespace(ch)) {
      return [idx, source.slice(start, idx)];
    }
  }

  return [scrubbed.length, source.slice(start)];
}

export function adaptArrayOperators(query: string): string {
  if (
    query.indexOf('@>') === -1 &&
    query.indexOf('<@') === -1 &&
    query.indexOf('&&') === -1
  ) {
    return query;
  }

  let rewritten = query;
  let scrubbed = scrubForRewrite(query);
  let searchStart = 0;

  // Re-run after each replacement to keep indexes aligned with the current string
  while (true) {
    const next = findNextOperator(scrubbed, searchStart);
    if (!next) break;

    const { index, operator } = next;
    const [leftStart, leftExpr] = walkLeft(rewritten, scrubbed, index - 1);
    const [rightEnd, rightExpr] = walkRight(
      rewritten,
      scrubbed,
      index + operator.token.length
    );

    const left = leftExpr.trim();
    const right = rightExpr.trim();

    const replacement = `${operator.fn}(${operator.swap ? right : left}, ${
      operator.swap ? left : right
    })`;

    rewritten =
      rewritten.slice(0, leftStart) + replacement + rewritten.slice(rightEnd);
    scrubbed = scrubForRewrite(rewritten);
    searchStart = leftStart + replacement.length;
  }

  return rewritten;
}

// Join column qualification types and helpers

type TableSource = {
  name: string; // The table/CTE name (without quotes)
  alias?: string; // Optional alias
  position: number; // Position in the query where this source was introduced
};

type JoinClause = {
  joinType: string; // 'left', 'right', 'inner', 'full', 'cross', ''
  tableName: string; // The joined table name
  tableAlias?: string; // Optional alias
  onStart: number; // Start of ON clause content (after "on ")
  onEnd: number; // End of ON clause content
  leftSource: string; // The table/alias on the left side of this join
  rightSource: string; // The table/alias for the joined table
};

/**
 * Extracts the identifier name from a quoted identifier like "foo" -> foo
 * Uses the original query string, not the scrubbed one.
 * Handles escaped quotes: "col""name" -> col""name
 */
function extractQuotedIdentifier(
  original: string,
  start: number
): { name: string; end: number } | null {
  if (original[start] !== '"') {
    return null;
  }

  let pos = start + 1;
  while (pos < original.length) {
    if (original[pos] === '"') {
      // Check for escaped quote ""
      if (original[pos + 1] === '"') {
        pos += 2;
        continue;
      }
      // End of identifier
      break;
    }
    pos++;
  }

  if (pos >= original.length) {
    return null;
  }

  return {
    name: original.slice(start + 1, pos),
    end: pos + 1,
  };
}

/**
 * Finds the main SELECT's FROM clause, skipping any FROM inside CTEs.
 */
function findMainFromClause(scrubbed: string): number {
  const lowerScrubbed = scrubbed.toLowerCase();

  // If there's a WITH clause, find where the CTEs end
  let searchStart = 0;
  const withMatch = /\bwith\s+/i.exec(lowerScrubbed);
  if (withMatch) {
    // Find the main SELECT after the CTEs
    // CTEs are separated by commas and end with the main SELECT
    let depth = 0;
    let pos = withMatch.index + withMatch[0].length;

    while (pos < scrubbed.length) {
      const char = scrubbed[pos];
      if (char === '(') {
        depth++;
      } else if (char === ')') {
        depth--;
      } else if (depth === 0) {
        // Check if we're at "select" keyword (main query)
        const remaining = lowerScrubbed.slice(pos);
        if (/^\s*select\s+/i.test(remaining)) {
          searchStart = pos;
          break;
        }
      }
      pos++;
    }
  }

  // Find FROM after the main SELECT
  const fromPattern = /\bfrom\s+/gi;
  fromPattern.lastIndex = searchStart;
  const fromMatch = fromPattern.exec(lowerScrubbed);

  return fromMatch ? fromMatch.index + fromMatch[0].length : -1;
}

/**
 * Parses table sources from FROM and JOIN clauses.
 * Returns an array of table sources in order of appearance.
 */
function parseTableSources(original: string, scrubbed: string): TableSource[] {
  const sources: TableSource[] = [];
  const lowerScrubbed = scrubbed.toLowerCase();

  // Find the main FROM clause (after CTEs if present)
  const fromPos = findMainFromClause(scrubbed);
  if (fromPos < 0) {
    return sources;
  }

  const fromTable = parseTableRef(original, scrubbed, fromPos);
  if (fromTable) {
    sources.push(fromTable);
  }

  const joinPattern =
    /\b(left\s+|right\s+|inner\s+|full\s+|cross\s+)?join\s+/gi;
  joinPattern.lastIndex = fromPos;

  let joinMatch;
  while ((joinMatch = joinPattern.exec(lowerScrubbed)) !== null) {
    const tableStart = joinMatch.index + joinMatch[0].length;
    const joinTable = parseTableRef(original, scrubbed, tableStart);
    if (joinTable) {
      sources.push(joinTable);
    }
  }

  return sources;
}

/**
 * Parses a table reference (potentially with alias) starting at the given position.
 * Handles: "table", "table" "alias", "table" as "alias", "schema"."table",
 * and subqueries: (SELECT ...) AS "alias"
 */
function parseTableRef(
  original: string,
  scrubbed: string,
  start: number
): TableSource | null {
  let pos = start;
  while (pos < scrubbed.length && isWhitespace(scrubbed[pos])) {
    pos++;
  }

  // Handle subquery: (SELECT ...) AS "alias"
  if (scrubbed[pos] === '(') {
    const nameStart = pos;
    // Find matching closing parenthesis
    let depth = 1;
    pos++;
    while (pos < scrubbed.length && depth > 0) {
      if (scrubbed[pos] === '(') depth++;
      else if (scrubbed[pos] === ')') depth--;
      pos++;
    }

    // Skip whitespace after subquery
    while (pos < scrubbed.length && isWhitespace(scrubbed[pos])) {
      pos++;
    }

    // Look for AS keyword
    const afterSubquery = scrubbed.slice(pos).toLowerCase();
    if (afterSubquery.startsWith('as ')) {
      pos += 3;
      while (pos < scrubbed.length && isWhitespace(scrubbed[pos])) {
        pos++;
      }
    }

    // Extract alias
    if (original[pos] === '"') {
      const aliasIdent = extractQuotedIdentifier(original, pos);
      if (aliasIdent) {
        return {
          name: aliasIdent.name,
          alias: aliasIdent.name,
          position: nameStart,
        };
      }
    }

    return null;
  }

  if (original[pos] !== '"') {
    return null;
  }

  const nameStart = pos;
  const firstIdent = extractQuotedIdentifier(original, pos);
  if (!firstIdent) {
    return null;
  }

  let name = firstIdent.name;
  pos = firstIdent.end;

  // Check for schema.table pattern
  let afterName = pos;
  while (afterName < scrubbed.length && isWhitespace(scrubbed[afterName])) {
    afterName++;
  }

  if (scrubbed[afterName] === '.') {
    afterName++;
    while (afterName < scrubbed.length && isWhitespace(scrubbed[afterName])) {
      afterName++;
    }
    if (original[afterName] === '"') {
      const tableIdent = extractQuotedIdentifier(original, afterName);
      if (tableIdent) {
        name = tableIdent.name;
        pos = tableIdent.end;
      }
    }
  }

  let alias: string | undefined;
  let aliasPos = pos;
  while (aliasPos < scrubbed.length && isWhitespace(scrubbed[aliasPos])) {
    aliasPos++;
  }

  const afterTable = scrubbed.slice(aliasPos).toLowerCase();
  if (afterTable.startsWith('as ')) {
    aliasPos += 3;
    while (aliasPos < scrubbed.length && isWhitespace(scrubbed[aliasPos])) {
      aliasPos++;
    }
  }

  // Check what comes after (potentially skipping the AS keyword)
  const afterAlias = scrubbed.slice(aliasPos).toLowerCase();
  if (
    original[aliasPos] === '"' &&
    !afterAlias.startsWith('on ') &&
    !afterAlias.startsWith('left ') &&
    !afterAlias.startsWith('right ') &&
    !afterAlias.startsWith('inner ') &&
    !afterAlias.startsWith('full ') &&
    !afterAlias.startsWith('cross ') &&
    !afterAlias.startsWith('join ') &&
    !afterAlias.startsWith('where ') &&
    !afterAlias.startsWith('group ') &&
    !afterAlias.startsWith('order ') &&
    !afterAlias.startsWith('limit ')
  ) {
    const aliasIdent = extractQuotedIdentifier(original, aliasPos);
    if (aliasIdent) {
      alias = aliasIdent.name;
    }
  }

  return {
    name,
    alias,
    position: nameStart,
  };
}

/**
 * Finds all JOIN clauses with their ON clause boundaries.
 */
function findJoinClauses(
  original: string,
  scrubbed: string,
  sources: TableSource[],
  fromPos: number
): JoinClause[] {
  const clauses: JoinClause[] = [];
  const lowerScrubbed = scrubbed.toLowerCase();

  // Pattern to find JOINs with ON clauses
  // Use scrubbed for matching, original for extracting values
  // Handle optional schema prefix: "schema"."table" or just "table"
  const joinPattern =
    /\b(left\s+|right\s+|inner\s+|full\s+|cross\s+)?join\s+"[^"]*"(\s*\.\s*"[^"]*")?(\s+as)?(\s+"[^"]*")?\s+on\s+/gi;

  // Start searching from the main FROM clause, skipping JOINs inside CTE definitions
  joinPattern.lastIndex = fromPos;

  let match;
  let sourceIndex = 1; // Start at 1 since index 0 is the FROM table

  while ((match = joinPattern.exec(lowerScrubbed)) !== null) {
    const joinType = (match[1] || '').trim().toLowerCase();
    const joinKeywordEnd =
      match.index + (match[1] || '').length + 'join'.length;
    let tableStart = joinKeywordEnd;
    while (tableStart < original.length && isWhitespace(original[tableStart])) {
      tableStart++;
    }

    const tableIdent = extractQuotedIdentifier(original, tableStart);
    if (!tableIdent) continue;

    let tableName = tableIdent.name;
    let afterTable = tableIdent.end;
    let checkPos = afterTable;
    while (checkPos < scrubbed.length && isWhitespace(scrubbed[checkPos])) {
      checkPos++;
    }
    if (scrubbed[checkPos] === '.') {
      checkPos++;
      while (checkPos < scrubbed.length && isWhitespace(scrubbed[checkPos])) {
        checkPos++;
      }
      const realTableIdent = extractQuotedIdentifier(original, checkPos);
      if (realTableIdent) {
        tableName = realTableIdent.name;
        afterTable = realTableIdent.end;
      }
    }

    let tableAlias: string | undefined;
    let aliasPos = afterTable;
    while (aliasPos < scrubbed.length && isWhitespace(scrubbed[aliasPos])) {
      aliasPos++;
    }

    const afterTableStr = scrubbed.slice(aliasPos).toLowerCase();
    if (afterTableStr.startsWith('as ')) {
      aliasPos += 3;
      while (aliasPos < scrubbed.length && isWhitespace(scrubbed[aliasPos])) {
        aliasPos++;
      }
    }

    if (original[aliasPos] === '"' && !afterTableStr.startsWith('on ')) {
      const aliasIdent = extractQuotedIdentifier(original, aliasPos);
      if (aliasIdent) {
        tableAlias = aliasIdent.name;
      }
    }

    const onStart = match.index + match[0].length;

    // Find the end of the ON clause (next JOIN, WHERE, GROUP, ORDER, LIMIT, or end)
    const endPattern =
      /\b(left\s+join|right\s+join|inner\s+join|full\s+join|cross\s+join|join|where|group\s+by|order\s+by|limit|$)/i;
    const remaining = lowerScrubbed.slice(onStart);
    const endMatch = endPattern.exec(remaining);
    const onEnd = endMatch ? onStart + endMatch.index : scrubbed.length;

    // Determine the left source (previous table/CTE)
    let leftSource = '';
    if (sourceIndex > 0 && sourceIndex <= sources.length) {
      const prev = sources[sourceIndex - 1];
      leftSource = prev?.alias || prev?.name || '';
    }

    const rightSource = tableAlias || tableName;

    clauses.push({
      joinType,
      tableName,
      tableAlias,
      onStart,
      onEnd,
      leftSource,
      rightSource,
    });

    sourceIndex++;
  }

  return clauses;
}

/**
 * Finds the boundaries of the main query's SELECT clause (after CTEs).
 * Returns the start position (after "select ") and end position (before "from ").
 */
function findMainSelectClause(
  scrubbed: string
): { start: number; end: number } | null {
  const lowerScrubbed = scrubbed.toLowerCase();

  // If there's a WITH clause, find where the CTEs end
  let searchStart = 0;
  const withMatch = /\bwith\s+/i.exec(lowerScrubbed);
  if (withMatch) {
    // Find the main SELECT after the CTEs
    let depth = 0;
    let pos = withMatch.index + withMatch[0].length;

    while (pos < scrubbed.length) {
      const char = scrubbed[pos];
      if (char === '(') {
        depth++;
      } else if (char === ')') {
        depth--;
      } else if (depth === 0) {
        const remaining = lowerScrubbed.slice(pos);
        if (/^\s*select\s+/i.test(remaining)) {
          searchStart = pos;
          break;
        }
      }
      pos++;
    }
  }

  // Find SELECT keyword
  const selectPattern = /\bselect\s+/gi;
  selectPattern.lastIndex = searchStart;
  const selectMatch = selectPattern.exec(lowerScrubbed);
  if (!selectMatch) return null;

  const selectStart = selectMatch.index + selectMatch[0].length;

  // Find FROM keyword at same depth
  let depth = 0;
  let pos = selectStart;
  while (pos < scrubbed.length) {
    const char = scrubbed[pos];
    if (char === '(') {
      depth++;
    } else if (char === ')') {
      depth--;
    } else if (depth === 0) {
      const remaining = lowerScrubbed.slice(pos);
      if (/^\s*from\s+/i.test(remaining)) {
        return { start: selectStart, end: pos };
      }
    }
    pos++;
  }

  return null;
}

/**
 * Finds WHERE clause boundaries in the main query.
 */
function findWhereClause(
  scrubbed: string,
  fromPos: number
): { start: number; end: number } | null {
  const lowerScrubbed = scrubbed.toLowerCase();

  // Find WHERE keyword after FROM, at depth 0
  let depth = 0;
  let pos = fromPos;
  let whereStart = -1;

  while (pos < scrubbed.length) {
    const char = scrubbed[pos];
    if (char === '(') {
      depth++;
    } else if (char === ')') {
      depth--;
    } else if (depth === 0) {
      const remaining = lowerScrubbed.slice(pos);
      if (whereStart === -1 && /^\s*where\s+/i.test(remaining)) {
        const match = /^\s*where\s+/i.exec(remaining);
        if (match) {
          whereStart = pos + match[0].length;
        }
      } else if (
        whereStart !== -1 &&
        /^\s*(group\s+by|order\s+by|limit|having|union|intersect|except|$)/i.test(
          remaining
        )
      ) {
        return { start: whereStart, end: pos };
      }
    }
    pos++;
  }

  if (whereStart !== -1) {
    return { start: whereStart, end: scrubbed.length };
  }

  return null;
}

/**
 * Finds ORDER BY clause boundaries in the main query.
 */
function findOrderByClause(
  scrubbed: string,
  fromPos: number
): { start: number; end: number } | null {
  const lowerScrubbed = scrubbed.toLowerCase();

  // Find ORDER BY keyword after FROM, at depth 0
  let depth = 0;
  let pos = fromPos;
  let orderStart = -1;

  while (pos < scrubbed.length) {
    const char = scrubbed[pos];
    if (char === '(') {
      depth++;
    } else if (char === ')') {
      depth--;
    } else if (depth === 0) {
      const remaining = lowerScrubbed.slice(pos);
      if (orderStart === -1 && /^\s*order\s+by\s+/i.test(remaining)) {
        const match = /^\s*order\s+by\s+/i.exec(remaining);
        if (match) {
          orderStart = pos + match[0].length;
        }
      } else if (
        orderStart !== -1 &&
        /^\s*(limit|offset|fetch|for\s+update|$)/i.test(remaining)
      ) {
        return { start: orderStart, end: pos };
      }
    }
    pos++;
  }

  if (orderStart !== -1) {
    return { start: orderStart, end: scrubbed.length };
  }

  return null;
}

/**
 * Qualifies only specific column references (from the ambiguousColumns set) in a clause.
 * This handles SELECT, WHERE, and ORDER BY clauses where columns from joined tables
 * could be ambiguous.
 */
function qualifyClauseColumnsSelective(
  original: string,
  scrubbed: string,
  clauseStart: number,
  clauseEnd: number,
  defaultSource: string,
  ambiguousColumns: Set<string>
): { result: string; offset: number } {
  const clauseOriginal = original.slice(clauseStart, clauseEnd);
  const clauseScrubbed = scrubbed.slice(clauseStart, clauseEnd);

  let result = clauseOriginal;
  let offset = 0;

  // Find all unqualified quoted identifiers
  let pos = 0;
  while (pos < clauseScrubbed.length) {
    // Skip to next quote
    const quotePos = clauseScrubbed.indexOf('"', pos);
    if (quotePos === -1) break;

    // Check if this is already qualified (preceded by a dot)
    if (quotePos > 0 && clauseScrubbed[quotePos - 1] === '.') {
      // Skip this identifier - it's already qualified
      const ident = extractQuotedIdentifier(clauseOriginal, quotePos);
      pos = ident ? ident.end : quotePos + 1;
      continue;
    }

    // Extract the identifier
    const ident = extractQuotedIdentifier(clauseOriginal, quotePos);
    if (!ident) {
      pos = quotePos + 1;
      continue;
    }

    // Check if this identifier is followed by a dot (it's a table qualifier, not a column)
    if (
      ident.end < clauseScrubbed.length &&
      clauseScrubbed[ident.end] === '.'
    ) {
      pos = ident.end + 1;
      continue;
    }

    // Only qualify if this column is in the ambiguous set
    if (!ambiguousColumns.has(ident.name)) {
      pos = ident.end;
      continue;
    }

    // Check if this looks like a column reference (not a function call or alias definition)
    // Skip if followed by ( which would indicate a function
    let afterIdent = ident.end;
    while (
      afterIdent < clauseScrubbed.length &&
      isWhitespace(clauseScrubbed[afterIdent])
    ) {
      afterIdent++;
    }
    if (clauseScrubbed[afterIdent] === '(') {
      pos = ident.end;
      continue;
    }

    // Skip if this is an alias definition (preceded by AS or follows a column expression)
    // Look for patterns like: "col" as "alias" or just "col", "alias"
    // We need to be careful here - we only want to qualify column references, not aliases
    const beforeQuote = clauseScrubbed.slice(0, quotePos).toLowerCase();
    if (/\bas\s*$/i.test(beforeQuote)) {
      pos = ident.end;
      continue;
    }

    // Qualify this column reference
    const qualified = `"${defaultSource}"."${ident.name}"`;
    const oldLength = ident.end - quotePos;

    result =
      result.slice(0, quotePos + offset) +
      qualified +
      result.slice(quotePos + oldLength + offset);

    offset += qualified.length - oldLength;
    pos = ident.end;
  }

  return { result, offset };
}

/**
 * Qualifies unqualified column references in JOIN ON clauses, SELECT, WHERE,
 * and ORDER BY clauses.
 *
 * Transforms patterns like:
 *   `select "col" from "a" left join "b" on "col" = "col" where "col" in (...)`
 * To:
 *   `select "a"."col" from "a" left join "b" on "a"."col" = "b"."col" where "a"."col" in (...)`
 *
 * This fixes the issue where drizzle-orm generates unqualified column
 * references when joining CTEs with eq().
 */
export function qualifyJoinColumns(query: string): string {
  const lowerQuery = query.toLowerCase();
  if (!lowerQuery.includes('join')) {
    return query;
  }

  const scrubbed = scrubForRewrite(query);
  const fromPos = findMainFromClause(scrubbed);
  if (fromPos < 0) {
    return query;
  }

  const sources = parseTableSources(query, scrubbed);
  if (sources.length < 2) {
    return query;
  }

  const joinClauses = findJoinClauses(query, scrubbed, sources, fromPos);

  if (joinClauses.length === 0) {
    return query;
  }

  // Get the first source (FROM table) as the default qualifier
  const firstSource = sources[0]!;
  const defaultQualifier = firstSource.alias || firstSource.name;

  let result = query;
  let totalOffset = 0;

  // First, qualify ON clauses (existing logic)
  for (const join of joinClauses) {
    const scrubbedOnClause = scrubbed.slice(join.onStart, join.onEnd);
    const originalOnClause = query.slice(join.onStart, join.onEnd);
    let clauseResult = originalOnClause;
    let clauseOffset = 0;
    let eqPos = -1;
    while ((eqPos = scrubbedOnClause.indexOf('=', eqPos + 1)) !== -1) {
      let lhsEnd = eqPos - 1;
      while (lhsEnd >= 0 && isWhitespace(scrubbedOnClause[lhsEnd])) {
        lhsEnd--;
      }
      if (scrubbedOnClause[lhsEnd] !== '"') continue;

      // Find start of identifier, handling escaped quotes ""
      let lhsStartPos = lhsEnd - 1;
      while (lhsStartPos >= 0) {
        if (scrubbedOnClause[lhsStartPos] === '"') {
          // Check if this is an escaped quote ""
          if (lhsStartPos > 0 && scrubbedOnClause[lhsStartPos - 1] === '"') {
            // Skip over the escaped quote pair
            lhsStartPos -= 2;
            continue;
          }
          // Found the opening quote
          break;
        }
        lhsStartPos--;
      }
      if (lhsStartPos < 0) continue;

      const lhsIsQualified =
        lhsStartPos > 0 && scrubbedOnClause[lhsStartPos - 1] === '.';
      let rhsStartPos = eqPos + 1;
      while (
        rhsStartPos < scrubbedOnClause.length &&
        isWhitespace(scrubbedOnClause[rhsStartPos])
      ) {
        rhsStartPos++;
      }

      const rhsChar = originalOnClause[rhsStartPos];
      const rhsIsParam = rhsChar === '$';
      const rhsIsStringLiteral = rhsChar === "'";
      const rhsIsColumn = rhsChar === '"';

      if (!rhsIsParam && !rhsIsStringLiteral && !rhsIsColumn) continue;

      const rhsIsQualified =
        !rhsIsColumn ||
        (rhsStartPos > 0 && scrubbedOnClause[rhsStartPos - 1] === '.');
      if (lhsIsQualified || rhsIsQualified) continue;

      const lhsIdent = extractQuotedIdentifier(originalOnClause, lhsStartPos);
      if (!lhsIdent) continue;

      let rhsIdent: { name: string; end: number } | null = null;
      let rhsValue = '';
      let rhsEnd = rhsStartPos;

      if (rhsIsParam) {
        let paramEnd = rhsStartPos + 1;
        while (
          paramEnd < originalOnClause.length &&
          /\d/.test(originalOnClause[paramEnd]!)
        ) {
          paramEnd++;
        }
        rhsValue = originalOnClause.slice(rhsStartPos, paramEnd);
        rhsEnd = paramEnd;
      } else if (rhsIsStringLiteral) {
        let literalEnd = rhsStartPos + 1;
        while (literalEnd < originalOnClause.length) {
          if (originalOnClause[literalEnd] === "'") {
            if (originalOnClause[literalEnd + 1] === "'") {
              literalEnd += 2;
              continue;
            }
            break;
          }
          literalEnd++;
        }
        rhsValue = originalOnClause.slice(rhsStartPos, literalEnd + 1);
        rhsEnd = literalEnd + 1;
      } else if (rhsIsColumn) {
        rhsIdent = extractQuotedIdentifier(originalOnClause, rhsStartPos);
        if (rhsIdent) {
          // Check if this identifier is followed by a dot (meaning it's a table prefix, not the column)
          if (
            rhsIdent.end < scrubbedOnClause.length &&
            scrubbedOnClause[rhsIdent.end] === '.'
          ) {
            // This is a qualified reference "table"."column" - skip, it's already qualified
            continue;
          }
          rhsValue = `"${rhsIdent.name}"`;
          rhsEnd = rhsIdent.end;
        }
      }

      if (!rhsValue) continue;

      // Only qualify when both sides are columns with the same name.
      // Only same-named columns cause "Ambiguous reference" errors in DuckDB.
      if (!rhsIsColumn || !rhsIdent || lhsIdent.name !== rhsIdent.name) {
        continue;
      }

      const lhsOriginal = `"${lhsIdent.name}"`;
      let newLhs = lhsOriginal;
      let newRhs = rhsValue;

      if (!lhsIsQualified && join.leftSource) {
        newLhs = `"${join.leftSource}"."${lhsIdent.name}"`;
      }

      if (!rhsIsQualified && rhsIsColumn && rhsIdent && join.rightSource) {
        newRhs = `"${join.rightSource}"."${rhsIdent.name}"`;
      }

      if (newLhs !== lhsOriginal || newRhs !== rhsValue) {
        const opStart = lhsIdent.end;
        let opEnd = opStart;
        while (
          opEnd < rhsEnd &&
          (isWhitespace(originalOnClause[opEnd]) ||
            originalOnClause[opEnd] === '=')
        ) {
          opEnd++;
        }
        const operator = originalOnClause.slice(opStart, opEnd);

        const newExpr = `${newLhs}${operator}${newRhs}`;
        const oldExprLength = rhsEnd - lhsStartPos;

        clauseResult =
          clauseResult.slice(0, lhsStartPos + clauseOffset) +
          newExpr +
          clauseResult.slice(lhsStartPos + oldExprLength + clauseOffset);

        clauseOffset += newExpr.length - oldExprLength;
      }
    }

    if (clauseResult !== originalOnClause) {
      result =
        result.slice(0, join.onStart + totalOffset) +
        clauseResult +
        result.slice(join.onEnd + totalOffset);
      totalOffset += clauseResult.length - originalOnClause.length;
    }
  }

  // Collect column names that are known to be ambiguous (appeared in ON clauses with same name on both sides)
  const ambiguousColumns = new Set<string>();
  for (const join of joinClauses) {
    const scrubbedOnClause = scrubbed.slice(join.onStart, join.onEnd);
    const originalOnClause = query.slice(join.onStart, join.onEnd);

    let eqPos = -1;
    while ((eqPos = scrubbedOnClause.indexOf('=', eqPos + 1)) !== -1) {
      let lhsEnd = eqPos - 1;
      while (lhsEnd >= 0 && isWhitespace(scrubbedOnClause[lhsEnd])) {
        lhsEnd--;
      }
      if (scrubbedOnClause[lhsEnd] !== '"') continue;

      let lhsStartPos = lhsEnd - 1;
      while (lhsStartPos >= 0) {
        if (scrubbedOnClause[lhsStartPos] === '"') {
          if (lhsStartPos > 0 && scrubbedOnClause[lhsStartPos - 1] === '"') {
            lhsStartPos -= 2;
            continue;
          }
          break;
        }
        lhsStartPos--;
      }
      if (lhsStartPos < 0) continue;

      let rhsStartPos = eqPos + 1;
      while (
        rhsStartPos < scrubbedOnClause.length &&
        isWhitespace(scrubbedOnClause[rhsStartPos])
      ) {
        rhsStartPos++;
      }

      if (originalOnClause[rhsStartPos] !== '"') continue;

      const lhsIdent = extractQuotedIdentifier(originalOnClause, lhsStartPos);
      const rhsIdent = extractQuotedIdentifier(originalOnClause, rhsStartPos);

      if (lhsIdent && rhsIdent && lhsIdent.name === rhsIdent.name) {
        ambiguousColumns.add(lhsIdent.name);
      }
    }
  }

  // If no ambiguous columns were found, we're done
  if (ambiguousColumns.size === 0) {
    return result;
  }

  // Now qualify SELECT, WHERE, and ORDER BY clauses - only for ambiguous columns
  // We need to re-scrub since the query may have changed
  const updatedScrubbed = scrubForRewrite(result);

  // Qualify SELECT clause
  const selectClause = findMainSelectClause(updatedScrubbed);
  if (selectClause) {
    const { result: selectResult, offset: selectOffset } =
      qualifyClauseColumnsSelective(
        result,
        updatedScrubbed,
        selectClause.start,
        selectClause.end,
        defaultQualifier,
        ambiguousColumns
      );
    if (selectOffset !== 0) {
      // Splice the modified clause back into the full query
      result =
        result.slice(0, selectClause.start) +
        selectResult +
        result.slice(selectClause.end);
    }
  }

  // Re-scrub after SELECT changes
  const scrubbed2 = scrubForRewrite(result);
  const fromPos2 = findMainFromClause(scrubbed2);

  // Qualify WHERE clause
  if (fromPos2 >= 0) {
    const whereClause = findWhereClause(scrubbed2, fromPos2);
    if (whereClause) {
      const { result: whereResult, offset: whereOffset } =
        qualifyClauseColumnsSelective(
          result,
          scrubbed2,
          whereClause.start,
          whereClause.end,
          defaultQualifier,
          ambiguousColumns
        );
      if (whereOffset !== 0) {
        // Splice the modified clause back into the full query
        result =
          result.slice(0, whereClause.start) +
          whereResult +
          result.slice(whereClause.end);
      }
    }
  }

  // Re-scrub after WHERE changes
  const scrubbed3 = scrubForRewrite(result);
  const fromPos3 = findMainFromClause(scrubbed3);

  // Qualify ORDER BY clause
  if (fromPos3 >= 0) {
    const orderByClause = findOrderByClause(scrubbed3, fromPos3);
    if (orderByClause) {
      const { result: orderResult, offset: orderOffset } =
        qualifyClauseColumnsSelective(
          result,
          scrubbed3,
          orderByClause.start,
          orderByClause.end,
          defaultQualifier,
          ambiguousColumns
        );
      if (orderOffset !== 0) {
        // Splice the modified clause back into the full query
        result =
          result.slice(0, orderByClause.start) +
          orderResult +
          result.slice(orderByClause.end);
      }
    }
  }

  return result;
}
