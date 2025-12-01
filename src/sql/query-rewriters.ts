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
