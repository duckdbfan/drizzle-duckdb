import { DuckDBInstance } from '@duckdb/node-api';
import { desc, eq, sql } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { drizzle, type DuckDBDatabase } from '../src/index.ts';

let db: DuckDBDatabase;
let instance: DuckDBInstance;
let connection: Awaited<ReturnType<DuckDBInstance['connect']>>;

const comparisons = pgTable('menu_comparisons', {
  comparisonId: integer('comparison_id').primaryKey(),
  comparisonName: text('comparison_name').notNull(),
});

beforeAll(async () => {
  instance = await DuckDBInstance.create(':memory:');
  connection = await instance.connect();
  db = drizzle(connection);

  await db.execute(
    sql`create table menu_comparisons (comparison_id integer primary key, comparison_name text)`
  );
  await db.execute(sql`insert into menu_comparisons values (1, 'a'), (2, 'b')`);
});

afterAll(async () => {
  connection.closeSync();
});

describe('CTE joins with snake and camel column names', () => {
  test('qualifies unqualified right side columns in ON clause', async () => {
    const primaryDetails = db.$with('primaryDetails').as(
      db
        .select({
          comparisonId: comparisons.comparisonId,
          foo: sql`1`.as('foo'),
        })
        .from(comparisons)
    );

    const competitorDetails = db.$with('competitorDetails').as(
      db
        .select({
          comparisonId: comparisons.comparisonId,
          bar: sql`2`.as('bar'),
        })
        .from(comparisons)
    );

    const result = await db
      .with(primaryDetails, competitorDetails)
      .select({
        comparisonId: comparisons.comparisonId,
        comparisonName: comparisons.comparisonName,
        foo: primaryDetails.foo,
        bar: competitorDetails.bar,
      })
      .from(comparisons)
      .innerJoin(
        primaryDetails,
        eq(comparisons.comparisonId, primaryDetails.comparisonId)
      )
      .innerJoin(
        competitorDetails,
        eq(comparisons.comparisonId, competitorDetails.comparisonId)
      )
      .orderBy(desc(comparisons.comparisonId));

    expect(result.map((r) => r.comparisonId)).toEqual([2, 1]);
  });
});
