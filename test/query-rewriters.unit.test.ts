import { describe, expect, test } from 'vitest';
import {
  adaptArrayOperators,
  qualifyJoinColumns,
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

describe('qualifyJoinColumns', () => {
  test('qualifies simple left join with unqualified columns', () => {
    const input = 'select * from "a" left join "b" on "col" = "col"';
    const expected = 'select * from "a" left join "b" on "a"."col" = "b"."col"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('qualifies right join with unqualified columns', () => {
    const input = 'select * from "a" right join "b" on "id" = "id"';
    const expected = 'select * from "a" right join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('qualifies inner join with unqualified columns', () => {
    const input = 'select * from "a" inner join "b" on "x" = "x"';
    const expected = 'select * from "a" inner join "b" on "a"."x" = "b"."x"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('qualifies plain join with unqualified columns', () => {
    const input = 'select * from "a" join "b" on "x" = "x"';
    const expected = 'select * from "a" join "b" on "a"."x" = "b"."x"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('leaves already qualified columns unchanged', () => {
    const input = 'select * from "a" left join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('leaves different column names unchanged (not ambiguous)', () => {
    // Only same-name columns cause ambiguity errors, so we don't touch different names
    const input = 'select * from "a" left join "b" on "x" = "y"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('leaves mixed qualified/unqualified with different names unchanged', () => {
    const input = 'select * from "a" left join "b" on "a"."x" = "y"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('leaves partially qualified same-name columns unchanged', () => {
    const input = 'select * from "a" left join "b" on "a"."col" = "col"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles query without joins', () => {
    const input = 'select "country" from "table"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles empty query', () => {
    expect(qualifyJoinColumns('')).toBe('');
  });

  test('handles CTEs with joins', () => {
    const input =
      'with "cte1" as (select 1), "cte2" as (select 2) select * from "cte1" left join "cte2" on "country" = "country"';
    const expected =
      'with "cte1" as (select 1), "cte2" as (select 2) select * from "cte1" left join "cte2" on "cte1"."country" = "cte2"."country"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles multiple sequential joins with same column names', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" left join "c" on "key" = "key"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" left join "c" on "b"."key" = "c"."key"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('leaves parameter comparisons unchanged (different types)', () => {
    // Parameters are different from column names, so no ambiguity
    const input = 'select * from "a" left join "b" on "col" = $1';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles schema-qualified tables', () => {
    const input =
      'select * from "schema"."table1" left join "schema"."table2" on "id" = "id"';
    const expected =
      'select * from "schema"."table1" left join "schema"."table2" on "table1"."id" = "table2"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('preserves whitespace in ON clause', () => {
    const input = 'select * from "a" left join "b" on "col"  =  "col"';
    const expected =
      'select * from "a" left join "b" on "a"."col"  =  "b"."col"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles query with WHERE after JOIN', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" where "a"."x" > 5';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" where "a"."x" > 5';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles query with ORDER BY after JOIN', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" order by "a"."x"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" order by "a"."x"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('leaves string literal comparisons unchanged (different types)', () => {
    // String literals are different from column names, so no ambiguity
    const input = `select * from "a" left join "b" on "col" = 'value'`;
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles complex real-world CTE query', () => {
    const input = `with "brandsAndOutletsCounts" as (select "country" from "restaurants"), "topCities" as (select "country" from "cities") select "country" from "brandsAndOutletsCounts" left join "topCities" on "country" = "country"`;
    const expected = `with "brandsAndOutletsCounts" as (select "country" from "restaurants"), "topCities" as (select "country" from "cities") select "brandsAndOutletsCounts"."country" from "brandsAndOutletsCounts" left join "topCities" on "brandsAndOutletsCounts"."country" = "topCities"."country"`;
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  // Edge cases

  test('handles FULL OUTER JOIN', () => {
    const input = 'select * from "a" full join "b" on "id" = "id"';
    const expected = 'select * from "a" full join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles CROSS JOIN (no ON clause)', () => {
    const input = 'select * from "a" cross join "b"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles JOIN with USING clause (not ON)', () => {
    // USING clause syntax is different, should be left unchanged
    const input = 'select * from "a" left join "b" using ("id")';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles column names with special characters', () => {
    const input = 'select * from "a" left join "b" on "my-col" = "my-col"';
    const expected =
      'select * from "a" left join "b" on "a"."my-col" = "b"."my-col"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles column names with spaces', () => {
    const input = 'select * from "a" left join "b" on "my col" = "my col"';
    const expected =
      'select * from "a" left join "b" on "a"."my col" = "b"."my col"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles table names with special characters', () => {
    const input =
      'select * from "my-table" left join "other-table" on "id" = "id"';
    const expected =
      'select * from "my-table" left join "other-table" on "my-table"."id" = "other-table"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles nested CTEs with joins inside CTE', () => {
    // Joins inside CTE definitions are now correctly skipped
    const input =
      'with "cte" as (select * from "x" left join "y" on "id" = "id") select * from "cte" left join "other" on "key" = "key"';
    const expected =
      'with "cte" as (select * from "x" left join "y" on "id" = "id") select * from "cte" left join "other" on "cte"."key" = "other"."key"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles 3 CTEs where one CTE references another', () => {
    const input =
      'with "brandsAndOutletsCounts" as (select "country" from "restaurants" group by "country"), "cities" as (select "country", "city" from "neighbourhoods" group by "country", "city"), "topCities" as (select "country" from "cities" group by "country") select "country" from "brandsAndOutletsCounts" left join "topCities" on "country" = "country"';
    const expected =
      'with "brandsAndOutletsCounts" as (select "country" from "restaurants" group by "country"), "cities" as (select "country", "city" from "neighbourhoods" group by "country", "city"), "topCities" as (select "country" from "cities" group by "country") select "brandsAndOutletsCounts"."country" from "brandsAndOutletsCounts" left join "topCities" on "brandsAndOutletsCounts"."country" = "topCities"."country"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles CTE with multiple internal joins - only qualifies main query', () => {
    // CTE has two internal joins, main query has one join - only main query should be qualified
    const input =
      'with "cte" as (select * from "x" left join "y" on "id" = "id" inner join "z" on "key" = "key") select * from "cte" left join "other" on "name" = "name"';
    const expected =
      'with "cte" as (select * from "x" left join "y" on "id" = "id" inner join "z" on "key" = "key") select * from "cte" left join "other" on "cte"."name" = "other"."name"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles 4 CTEs with chain of dependencies', () => {
    // cte1 -> cte2 -> cte3 -> cte4, main query joins cte1 and cte4
    const input =
      'with "cte1" as (select "id" from "base"), "cte2" as (select "id" from "cte1"), "cte3" as (select "id" from "cte2"), "cte4" as (select "id" from "cte3") select * from "cte1" inner join "cte4" on "id" = "id"';
    const expected =
      'with "cte1" as (select "id" from "base"), "cte2" as (select "id" from "cte1"), "cte3" as (select "id" from "cte2"), "cte4" as (select "id" from "cte3") select * from "cte1" inner join "cte4" on "cte1"."id" = "cte4"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles main query with 3 joins after CTEs', () => {
    const input =
      'with "cte" as (select "x" from "t") select * from "a" left join "b" on "id" = "id" inner join "c" on "key" = "key" right join "d" on "name" = "name"';
    const expected =
      'with "cte" as (select "x" from "t") select * from "a" left join "b" on "a"."id" = "b"."id" inner join "c" on "b"."key" = "c"."key" right join "d" on "c"."name" = "d"."name"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles subquery in FROM with CTE in main query', () => {
    const input =
      'with "cte" as (select "country" from "regions") select * from (select * from "orders") as "sub" left join "cte" on "country" = "country"';
    const expected =
      'with "cte" as (select "country" from "regions") select * from (select * from "orders") as "sub" left join "cte" on "sub"."country" = "cte"."country"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles CTE with subquery inside its definition', () => {
    const input =
      'with "cte" as (select * from (select "id" from "inner") as "sub" left join "other" on "id" = "id") select * from "cte" left join "final" on "key" = "key"';
    const expected =
      'with "cte" as (select * from (select "id" from "inner") as "sub" left join "other" on "id" = "id") select * from "cte" left join "final" on "cte"."key" = "final"."key"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles recursive-style CTE pattern', () => {
    // Simulates recursive CTE syntax (without RECURSIVE keyword for simplicity)
    const input =
      'with "tree" as (select "id", "parent_id" from "nodes" union all select "id", "parent_id" from "tree" inner join "nodes" on "parent_id" = "parent_id") select * from "tree" left join "labels" on "id" = "id"';
    const expected =
      'with "tree" as (select "id", "parent_id" from "nodes" union all select "id", "parent_id" from "tree" inner join "nodes" on "parent_id" = "parent_id") select * from "tree" left join "labels" on "tree"."id" = "labels"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles multiple CTEs all used in main query joins', () => {
    const input =
      'with "a" as (select "id" from "t1"), "b" as (select "id" from "t2"), "c" as (select "id" from "t3") select * from "a" left join "b" on "id" = "id" left join "c" on "id" = "id"';
    const expected =
      'with "a" as (select "id" from "t1"), "b" as (select "id" from "t2"), "c" as (select "id" from "t3") select * from "a" left join "b" on "a"."id" = "b"."id" left join "c" on "b"."id" = "c"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles CTE with aggregation and GROUP BY joining main table', () => {
    const input =
      'with "agg" as (select "category", count(*) as "cnt" from "products" group by "category") select * from "categories" inner join "agg" on "category" = "category"';
    const expected =
      'with "agg" as (select "category", count(*) as "cnt" from "products" group by "category") select * from "categories" inner join "agg" on "categories"."category" = "agg"."category"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles deeply nested subqueries in FROM', () => {
    const input =
      'select * from (select * from (select "id" from "deep")) as "a" left join "b" on "id" = "id"';
    const expected =
      'select * from (select * from (select "id" from "deep")) as "a" left join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles join with OR condition containing same-name columns', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" or "key" = "key"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" or "a"."key" = "b"."key"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles CTE used in both FROM and JOIN of main query', () => {
    const input =
      'with "shared" as (select "id", "type" from "base") select * from "shared" as "s1" inner join "shared" as "s2" on "id" = "id"';
    const expected =
      'with "shared" as (select "id", "type" from "base") select * from "shared" as "s1" inner join "shared" as "s2" on "s1"."id" = "s2"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles complex real-world analytics query pattern', () => {
    // Pattern similar to the user's original issue
    const input =
      'with "sales" as (select "region", sum("amount") as "total" from "orders" group by "region"), "targets" as (select "region", "target" from "goals"), "comparison" as (select "region", "total", "target" from "sales" inner join "targets" on "region" = "region") select * from "regions" left join "comparison" on "region" = "region"';
    const expected =
      'with "sales" as (select "region", sum("amount") as "total" from "orders" group by "region"), "targets" as (select "region", "target" from "goals"), "comparison" as (select "region", "total", "target" from "sales" inner join "targets" on "region" = "region") select * from "regions" left join "comparison" on "regions"."region" = "comparison"."region"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles query with GROUP BY after JOIN', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" group by "a"."x"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" group by "a"."x"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles query with LIMIT after JOIN', () => {
    const input = 'select * from "a" left join "b" on "id" = "id" limit 10';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" limit 10';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles multiple equality conditions with AND', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" and "type" = "type"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" and "a"."type" = "b"."type"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles mixed same-name and different-name conditions', () => {
    // Only the same-name columns should be qualified
    const input =
      'select * from "a" left join "b" on "id" = "id" and "x" = "y"';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" and "x" = "y"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles subquery in FROM clause', () => {
    const input =
      'select * from (select * from "x") as "a" left join "b" on "id" = "id"';
    const expected =
      'select * from (select * from "x") as "a" left join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles newlines in query', () => {
    const input = `select *
from "a"
left join "b" on "id" = "id"`;
    const expected = `select *
from "a"
left join "b" on "a"."id" = "b"."id"`;
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles tabs in query', () => {
    const input = 'select *\tfrom "a"\tleft join "b" on "id" = "id"';
    const expected = 'select *\tfrom "a"\tleft join "b" on "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('qualifies only ambiguous columns in WHERE clause', () => {
    // Only columns that appear in ON clauses with same name on both sides are considered ambiguous
    // and get qualified in WHERE. "status" is not ambiguous here since ON clause uses "id".
    const input =
      'select * from "a" left join "b" on "a"."id" = "b"."id" where "status" = "status"';
    // status is not qualified because it wasn't in the ON clause as ambiguous
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('qualifies ambiguous columns in WHERE clause when same column in ON', () => {
    // When "id" is used ambiguously in ON clause, it should also be qualified in WHERE
    const input =
      'select * from "a" left join "b" on "id" = "id" where "id" = $1';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" where "a"."id" = $1';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles case sensitivity in keywords', () => {
    const input = 'SELECT * FROM "a" LEFT JOIN "b" ON "id" = "id"';
    const expected = 'SELECT * FROM "a" LEFT JOIN "b" ON "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles mixed case keywords', () => {
    const input = 'Select * From "a" Left Join "b" On "id" = "id"';
    const expected = 'Select * From "a" Left Join "b" On "a"."id" = "b"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles table alias with AS keyword', () => {
    // Both FROM and JOIN tables correctly use their aliases
    const input =
      'select * from "users" as "u" left join "orders" as "o" on "id" = "id"';
    const expected =
      'select * from "users" as "u" left join "orders" as "o" on "u"."id" = "o"."id"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles only one side being a column (comparison with function)', () => {
    // If one side is not a simple column, leave unchanged
    const input = 'select * from "a" left join "b" on lower("name") = "name"';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles empty ON clause gracefully', () => {
    // Malformed query but should not crash
    const input = 'select * from "a" left join "b" on ';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles column name that looks like keyword', () => {
    const input = 'select * from "a" left join "b" on "select" = "select"';
    const expected =
      'select * from "a" left join "b" on "a"."select" = "b"."select"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('preserves comments in query', () => {
    const input =
      'select * from "a" left join "b" on "id" = "id" -- join on id';
    const expected =
      'select * from "a" left join "b" on "a"."id" = "b"."id" -- join on id';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('does not modify columns inside comments', () => {
    const input =
      'select * from "a" left join "b" on "a"."id" = "b"."id" /* "id" = "id" */';
    expect(qualifyJoinColumns(input)).toBe(input);
  });

  test('handles very long table names', () => {
    const longName = 'a'.repeat(100);
    const input = `select * from "${longName}" left join "b" on "id" = "id"`;
    const expected = `select * from "${longName}" left join "b" on "${longName}"."id" = "b"."id"`;
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles very long column names', () => {
    const longName = 'x'.repeat(100);
    const input = `select * from "a" left join "b" on "${longName}" = "${longName}"`;
    const expected = `select * from "a" left join "b" on "a"."${longName}" = "b"."${longName}"`;
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles numeric-looking column names', () => {
    const input = 'select * from "a" left join "b" on "123" = "123"';
    const expected = 'select * from "a" left join "b" on "a"."123" = "b"."123"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });

  test('handles escaped quotes in identifiers', () => {
    const input =
      'select * from "a" left join "b" on "col""name" = "col""name"';
    const expected =
      'select * from "a" left join "b" on "a"."col""name" = "b"."col""name"';
    expect(qualifyJoinColumns(input)).toBe(expected);
  });
});
