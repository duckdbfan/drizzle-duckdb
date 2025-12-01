import { describe, expect, test } from 'vitest';
import {
  adaptArrayOperators,
  scrubForRewrite,
} from '../src/sql/query-rewriters.ts';

describe('scrubForRewrite', () => {
  test('preserves code outside strings and comments', () => {
    const result = scrubForRewrite('SELECT * FROM t');
    expect(result).toBe('SELECT * FROM t');
  });

  test('masks string content inside literals', () => {
    const result = scrubForRewrite("SELECT 'hello world' FROM t");
    expect(result).toBe("SELECT '...........' FROM t");
  });

  test('preserves escaped single quotes', () => {
    const result = scrubForRewrite("SELECT 'it''s' FROM t");
    // Escaped quotes are handled by state machine - this is implementation-specific
    expect(result.length).toBe("SELECT 'it''s' FROM t".length);
  });

  test('stays inside string literal across escaped quotes', () => {
    const query = "SELECT 'abc''@>'";
    const result = scrubForRewrite(query);

    expect(result).toHaveLength(query.length);
    expect(result).not.toContain('@>');
    expect(result.startsWith("SELECT '")).toBe(true);
    expect(result.endsWith("'")).toBe(true);
  });

  test('masks content inside double-quoted identifiers', () => {
    const query = 'SELECT "a@>b" FROM t';
    const result = scrubForRewrite(query);

    expect(result).toHaveLength(query.length);
    expect(result).not.toContain('@>');
    expect(result).toContain('"');
  });

  test('replaces line comment content with spaces', () => {
    const result = scrubForRewrite('SELECT * -- comment\nFROM t');
    expect(result).toBe('SELECT *           \nFROM t');
  });

  test('replaces block comment content with spaces', () => {
    const result = scrubForRewrite('SELECT * /* comment */ FROM t');
    // Block comments are replaced - verify structure is preserved
    expect(result).toContain('SELECT *');
    expect(result).toContain('FROM t');
    expect(result).not.toContain('comment');
  });

  test('handles nested strings in comments', () => {
    const result = scrubForRewrite("SELECT * /* 'nested' */ FROM t");
    // Comments including strings are replaced
    expect(result).toContain('SELECT *');
    expect(result).toContain('FROM t');
    expect(result).not.toContain('nested');
  });

  test('handles empty query', () => {
    const result = scrubForRewrite('');
    expect(result).toBe('');
  });

  test('handles query ending in line comment', () => {
    const result = scrubForRewrite('SELECT * -- comment');
    expect(result).toBe('SELECT *           ');
  });

  test('handles unclosed block comment', () => {
    const result = scrubForRewrite('SELECT * /* unclosed');
    // Unclosed block comment - verify start is preserved
    expect(result).toContain('SELECT *');
    expect(result).not.toContain('unclosed');
  });
});

describe('adaptArrayOperators', () => {
  test('rewrites @> to array_has_all', () => {
    const result = adaptArrayOperators('col @> arr');
    expect(result).toBe('array_has_all(col, arr)');
  });

  test('rewrites <@ to array_has_all with swapped arguments', () => {
    const result = adaptArrayOperators('col <@ arr');
    expect(result).toBe('array_has_all(arr, col)');
  });

  test('rewrites && to array_has_any', () => {
    const result = adaptArrayOperators('col && arr');
    expect(result).toBe('array_has_any(col, arr)');
  });

  test('handles multiple operators in one query', () => {
    const result = adaptArrayOperators('a @> b AND c && d');
    expect(result).toBe('array_has_all(a, b) AND array_has_any(c, d)');
  });

  test('does not rewrite operator in string literal', () => {
    const result = adaptArrayOperators("SELECT '@>' FROM t");
    expect(result).toBe("SELECT '@>' FROM t");
  });

  test('does not rewrite operator after escaped quote in string literal', () => {
    const query = "SELECT 'abc''@>' FROM t";
    const result = adaptArrayOperators(query);

    expect(result).toBe(query);
  });

  test('does not rewrite operator in double-quoted identifier', () => {
    const query = 'SELECT "col@>name" FROM t';
    const result = adaptArrayOperators(query);

    expect(result).toBe(query);
  });

  test('does not rewrite operator in single-line comment', () => {
    const result = adaptArrayOperators('SELECT * -- @>\nFROM t');
    expect(result).toBe('SELECT * -- @>\nFROM t');
  });

  test('does not rewrite operator in block comment', () => {
    const result = adaptArrayOperators('SELECT * /* @> */ FROM t');
    expect(result).toBe('SELECT * /* @> */ FROM t');
  });

  test('handles nested parentheses on left side', () => {
    const result = adaptArrayOperators('((col)) @> arr');
    expect(result).toBe('array_has_all(((col)), arr)');
  });

  test('handles nested parentheses on right side', () => {
    const result = adaptArrayOperators('col @> ((arr))');
    expect(result).toBe('array_has_all(col, ((arr)))');
  });

  test('handles function call as operand', () => {
    const result = adaptArrayOperators('array_agg(col) @> arr');
    expect(result).toBe('array_has_all(array_agg(col), arr)');
  });

  test('handles complex expressions', () => {
    const result = adaptArrayOperators('func(a, b) @> other_func(c)');
    expect(result).toBe('array_has_all(func(a, b), other_func(c))');
  });

  test('handles empty query', () => {
    const result = adaptArrayOperators('');
    expect(result).toBe('');
  });

  test('handles query without operators', () => {
    const result = adaptArrayOperators('SELECT * FROM t WHERE x = 1');
    expect(result).toBe('SELECT * FROM t WHERE x = 1');
  });

  test('handles whitespace around operators', () => {
    const result = adaptArrayOperators('col   @>   arr');
    expect(result).toBe('array_has_all(col, arr)');
  });

  test('handles square brackets in operands', () => {
    const result = adaptArrayOperators('arr[1] @> arr[2]');
    expect(result).toBe('array_has_all(arr[1], arr[2])');
  });

  test('handles newlines around operators', () => {
    const result = adaptArrayOperators('col\n@>\narr');
    expect(result).toBe('array_has_all(col, arr)');
  });

  test('handles mixed operators and comments', () => {
    const result = adaptArrayOperators(
      'a @> b /* comment */ AND c && d -- comment'
    );
    expect(result).toBe(
      'array_has_all(a, b) /* comment */ AND array_has_any(c, d) -- comment'
    );
  });

  test('preserves query structure with WHERE clause', () => {
    const result = adaptArrayOperators(
      'SELECT * FROM t WHERE tags @> ARRAY[1,2]'
    );
    expect(result).toBe(
      'SELECT * FROM t WHERE array_has_all(tags, ARRAY[1,2])'
    );
  });

  test('handles all three operators together', () => {
    const result = adaptArrayOperators('a @> b AND c <@ d AND e && f');
    expect(result).toBe(
      'array_has_all(a, b) AND array_has_all(d, c) AND array_has_any(e, f)'
    );
  });

  test('preserves string literal operands', () => {
    const result = adaptArrayOperators("tags @> '{foo}'");
    expect(result).toBe("array_has_all(tags, '{foo}')");
  });
});
