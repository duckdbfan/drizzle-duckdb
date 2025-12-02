/**
 * Tests for the new AST-based SQL transformer.
 *
 * These tests verify that the AST transformer correctly transforms:
 * 1. Array operators (@>, <@, &&) to DuckDB functions
 * 2. JOIN column qualification for ambiguous columns
 */

import { describe, expect, it } from 'vitest';
import {
  transformSQL,
  needsTransformation,
} from '../src/sql/ast-transformer.ts';

describe('transformSQL', () => {
  describe('array operator transformation', () => {
    it('transforms @> to array_has_all', () => {
      const result = transformSQL('SELECT * FROM t WHERE tags @> ARRAY[1,2]');
      expect(result.transformed).toBe(true);
      expect(result.sql.toLowerCase()).toContain('array_has_all');
      expect(result.sql).not.toContain('@>');
    });

    it('transforms <@ to array_has_all with swapped arguments', () => {
      const result = transformSQL('SELECT * FROM t WHERE tags <@ ARRAY[1,2]');
      expect(result.transformed).toBe(true);
      expect(result.sql.toLowerCase()).toContain('array_has_all');
      expect(result.sql).not.toContain('<@');
    });

    it('transforms && to array_has_any', () => {
      const result = transformSQL('SELECT * FROM t WHERE tags && ARRAY[1,2]');
      expect(result.transformed).toBe(true);
      expect(result.sql.toLowerCase()).toContain('array_has_any');
      expect(result.sql).not.toContain('&&');
    });

    it('handles multiple array operators', () => {
      const result = transformSQL(
        'SELECT * FROM t WHERE tags @> ARRAY[1] AND tags && ARRAY[2]'
      );
      expect(result.transformed).toBe(true);
      expect(result.sql.toLowerCase()).toContain('array_has_all');
      expect(result.sql.toLowerCase()).toContain('array_has_any');
    });
  });

  describe('JOIN column qualification', () => {
    it('qualifies unqualified columns in simple JOIN', () => {
      const result = transformSQL(
        'SELECT * FROM "a" LEFT JOIN "b" ON "id" = "id"'
      );
      // The AST transformer should qualify the columns
      expect(result.sql).toContain('"a"');
      expect(result.sql).toContain('"b"');
    });

    it('does not transform queries without JOINs', () => {
      const result = transformSQL('SELECT * FROM users WHERE id = 1');
      expect(result.transformed).toBe(false);
      expect(result.sql).toBe('SELECT * FROM users WHERE id = 1');
    });

    it('does not transform queries without array operators', () => {
      const result = transformSQL('SELECT * FROM users');
      expect(result.transformed).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('returns original SQL for empty string', () => {
      const result = transformSQL('');
      expect(result.sql).toBe('');
      expect(result.transformed).toBe(false);
    });

    it('handles complex SELECT statements', () => {
      const sql = `
        WITH cte AS (SELECT id FROM users)
        SELECT * FROM cte c
        LEFT JOIN posts p ON c.id = p.user_id
        WHERE p.tags @> ARRAY['featured']
      `;
      const result = transformSQL(sql);
      // Should at least transform the array operator
      expect(result.sql.toLowerCase()).toContain('array_has_all');
    });

    it('falls back gracefully for unparseable SQL', () => {
      // This is invalid SQL that the parser won't understand
      const sql = 'THIS IS NOT SQL @> AT ALL';
      const result = transformSQL(sql);
      // Should return original SQL without error
      expect(result.sql).toBe(sql);
      expect(result.transformed).toBe(false);
    });
  });
});

describe('needsTransformation', () => {
  it('returns true for queries with @>', () => {
    expect(needsTransformation('SELECT * FROM t WHERE a @> b')).toBe(true);
  });

  it('returns true for queries with <@', () => {
    expect(needsTransformation('SELECT * FROM t WHERE a <@ b')).toBe(true);
  });

  it('returns true for queries with &&', () => {
    expect(needsTransformation('SELECT * FROM t WHERE a && b')).toBe(true);
  });

  it('returns true for queries with JOIN', () => {
    expect(needsTransformation('SELECT * FROM a JOIN b ON a.id = b.id')).toBe(
      true
    );
  });

  it('returns true for queries with LEFT JOIN', () => {
    expect(
      needsTransformation('SELECT * FROM a LEFT JOIN b ON a.id = b.id')
    ).toBe(true);
  });

  it('returns false for simple SELECT', () => {
    expect(needsTransformation('SELECT * FROM users')).toBe(false);
  });

  it('returns false for INSERT', () => {
    expect(needsTransformation('INSERT INTO users (id) VALUES (1)')).toBe(
      false
    );
  });
});
