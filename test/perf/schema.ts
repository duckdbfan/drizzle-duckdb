import {
  boolean,
  doublePrecision,
  integer,
  pgTable,
  text,
} from 'drizzle-orm/pg-core';
import { duckDbStruct, duckDbMap } from '../../src/columns.ts';

export const factLarge = pgTable('fact_large', {
  id: integer('id').primaryKey(),
  mod100: integer('mod100').notNull(),
  cat: integer('cat').notNull(),
  value: doublePrecision('value').notNull(),
  payload: text('payload').notNull(),
});

export const narrowWide = pgTable('narrow_wide', {
  id: integer('id').primaryKey(),
  flag: integer('flag').notNull(),
  t1: text('t1').notNull(),
  t2: text('t2').notNull(),
  m1: doublePrecision('m1').notNull(),
  m2: doublePrecision('m2').notNull(),
  smallInt: integer('small_int').notNull(),
  isMultipleOf3: boolean('is_multiple_of_3').notNull(),
});

export const arrayTable = pgTable('array_table', {
  id: integer('id').primaryKey(),
  seqArray: integer('seq_array').array().notNull(),
  tagList: text('tag_list').array().notNull(),
});

export const benchInsert = pgTable('bench_insert', {
  id: integer('id').primaryKey(),
  val: text('val').notNull(),
});

export const benchPrepared = pgTable('bench_prepared', {
  id: integer('id').primaryKey(),
  val: text('val').notNull(),
});

export const benchComplex = pgTable('bench_complex', {
  id: integer('id'),
  meta: duckDbStruct<{ version: number; tag: string }>('meta', {
    version: 'INTEGER',
    tag: 'TEXT',
  }),
  attrs: duckDbMap<Record<string, string>>('attrs', 'TEXT'),
  nums: integer('nums').array().notNull(),
  tags: text('tags').array().notNull(),
});
