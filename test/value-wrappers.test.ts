import { expect, test } from 'vitest';
import {
  wrapList,
  wrapTimestamp,
  wrapperToNodeApiValue,
} from '../src/value-wrappers.ts';

test('wrapperToNodeApiValue handles nested wrappers', () => {
  const nested = wrapList([wrapList([1, 2]), 3]);
  const value = wrapperToNodeApiValue(nested, (v) => v as any);

  expect(value).toBeDefined();
});

test('wrapTimestamp accepts bigint microseconds', () => {
  const micros = 1_700_000_000_000_000n;
  const wrapped = wrapTimestamp(micros, false);
  const value = wrapperToNodeApiValue(wrapped, (v) => v as any);

  expect(value).toHaveProperty('micros', micros);
});

test('wrapperToNodeApiValue throws on unknown wrapper kind', () => {
  const bad = {
    kind: 'bad-kind',
    data: null,
    [Symbol.for('drizzle-duckdb:value')]: true,
  } as any;

  expect(() => wrapperToNodeApiValue(bad, (v) => v as any)).toThrow(
    /unknown wrapper/i
  );
});
