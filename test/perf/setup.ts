import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { drizzle, type DuckDBDatabase } from '../../src/index.ts';
import { closeClientConnection } from '../../src/client.ts';

export interface PerfHarness {
  connection: DuckDBConnection;
  db: DuckDBDatabase;
}

export async function createPerfHarness(): Promise<PerfHarness> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();

  await seedFactTable(connection);
  await seedNarrowWide(connection);
  await seedArrayTable(connection);
  await createInsertTable(connection);
  await createPreparedTable(connection);
  await createComplexTable(connection);

  const db = drizzle(connection);

  return { connection, db };
}

export async function closePerfHarness(harness: PerfHarness): Promise<void> {
  await closeClientConnection(harness.connection);
}

async function seedFactTable(connection: DuckDBConnection): Promise<void> {
  await connection.run(`
    create table fact_large as
    select
      i as id,
      i % 100 as mod100,
      i % 10 as cat,
      (i * 0.1)::double as value,
      concat('payload-', i) as payload
    from range(0, 100000) as t(i);
  `);

  await connection.run(
    'create index fact_large_mod_idx on fact_large(mod100);'
  );
}

async function seedNarrowWide(connection: DuckDBConnection): Promise<void> {
  await connection.run(`
    create table narrow_wide as
    select
      i as id,
      i % 5 as flag,
      concat('txt-', i % 20) as t1,
      concat('txt-', (i + 1) % 20) as t2,
      (i * 1.5)::double as m1,
      ((i % 7) * 3)::double as m2,
      (i % 11) as small_int,
      (i % 3 = 0) as is_multiple_of_3
    from range(0, 2000) as t(i);
  `);
}

async function seedArrayTable(connection: DuckDBConnection): Promise<void> {
  await connection.run(`
    create table array_table as
    select
      i as id,
      [i, i + 1, i + 2]::integer[] as seq_array,
      [concat('a-', i), concat('b-', i)] as tag_list
    from range(0, 5000) as t(i);
  `);
}

async function createInsertTable(connection: DuckDBConnection): Promise<void> {
  await connection.run(`
    drop table if exists bench_insert;
    create table bench_insert (
      id integer primary key,
      val text
    );
  `);
}

async function createPreparedTable(
  connection: DuckDBConnection
): Promise<void> {
  await connection.run(`
    drop table if exists bench_prepared;
    create table bench_prepared as
    select i as id, concat('val-', i) as val
    from range(0, 200) as t(i);
  `);
}

async function createComplexTable(connection: DuckDBConnection): Promise<void> {
  await connection.run(`
    drop table if exists bench_complex;
    create table bench_complex (
      id integer,
      meta struct(version integer, tag text),
      attrs map(text, text),
      nums integer[],
      tags text[]
    );
  `);
}
