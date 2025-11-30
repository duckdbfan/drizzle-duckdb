export function adaptArrayOperators(query: string): string {
  type ArrayOperator = {
    token: '@>' | '<@' | '&&';
    fn: 'array_has_all' | 'array_has_any';
    swap?: boolean;
  };

  const operators: ArrayOperator[] = [
    { token: '@>', fn: 'array_has_all' },
    { token: '<@', fn: 'array_has_all', swap: true },
    { token: '&&', fn: 'array_has_any' },
  ];

  const isWhitespace = (char: string | undefined) =>
    char !== undefined && /\s/.test(char);

  const walkLeft = (source: string, start: number): [number, string] => {
    let idx = start;
    while (idx >= 0 && isWhitespace(source[idx])) {
      idx--;
    }

    let depth = 0;
    let inString = false;
    for (; idx >= 0; idx--) {
      const ch = source[idx];
      if (ch === undefined) break;
      if (ch === "'" && source[idx - 1] !== '\\') {
        inString = !inString;
      }
      if (inString) continue;
      if (ch === ')' || ch === ']') {
        depth++;
      } else if (ch === '(' || ch === '[') {
        depth--;
        if (depth < 0) {
          return [idx + 1, source.slice(idx + 1, start + 1)];
        }
      } else if (depth === 0 && isWhitespace(ch)) {
        return [idx + 1, source.slice(idx + 1, start + 1)];
      }
    }
    return [0, source.slice(0, start + 1)];
  };

  const walkRight = (source: string, start: number): [number, string] => {
    let idx = start;
    while (idx < source.length && isWhitespace(source[idx])) {
      idx++;
    }

    let depth = 0;
    let inString = false;
    for (; idx < source.length; idx++) {
      const ch = source[idx];
      if (ch === undefined) break;
      if (ch === "'" && source[idx - 1] !== '\\') {
        inString = !inString;
      }
      if (inString) continue;
      if (ch === '(' || ch === '[') {
        depth++;
      } else if (ch === ')' || ch === ']') {
        depth--;
        if (depth < 0) {
          return [idx, source.slice(start, idx)];
        }
      } else if (depth === 0 && isWhitespace(ch)) {
        return [idx, source.slice(start, idx)];
      }
    }
    return [source.length, source.slice(start)];
  };

  let rewritten = query;
  for (const { token, fn, swap } of operators) {
    let idx = rewritten.indexOf(token);
    while (idx !== -1) {
      const [leftStart, leftExpr] = walkLeft(rewritten, idx - 1);
      const [rightEnd, rightExpr] = walkRight(rewritten, idx + token.length);

      const left = leftExpr.trim();
      const right = rightExpr.trim();

      const replacement = `${fn}(${swap ? right : left}, ${
        swap ? left : right
      })`;

      rewritten =
        rewritten.slice(0, leftStart) + replacement + rewritten.slice(rightEnd);

      idx = rewritten.indexOf(token, leftStart + replacement.length);
    }
  }

  return rewritten;
}
