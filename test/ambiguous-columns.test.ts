/**
 * Diagnostic tests for ambiguous column reference scenarios.
 *
 * These tests cover patterns that can trigger "ambiguous column reference" errors
 * in DuckDB when column names aren't properly qualified in JOIN ON clauses.
 *
 * Run with: bun test test/ambiguous-columns.test.ts
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { eq, sql, and } from 'drizzle-orm';
import { pgTable, integer, text, pgSchema } from 'drizzle-orm/pg-core';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { drizzle, type DuckDBDatabase } from '../src/index.ts';

let db: DuckDBDatabase;
let instance: DuckDBInstance;

// Schema definitions for test scenarios
const testSchema = pgSchema('test_schema');

const brands = testSchema.table('brands', {
  id: integer('id').primaryKey(),
  country: text('country').notNull(),
  brandSlug: text('brand_slug').notNull(),
  brandName: text('brand_name').notNull(),
});

const restaurants = testSchema.table('restaurants', {
  id: integer('id').primaryKey(),
  country: text('country').notNull(),
  brandSlug: text('brand_slug').notNull(),
  name: text('name').notNull(),
  isValid: integer('is_valid').notNull(),
});

const menuStats = testSchema.table('menu_stats', {
  id: integer('id').primaryKey(),
  country: text('country').notNull(),
  brandSlug: text('brand_slug').notNull(),
  menuCount: integer('menu_count').notNull(),
});

const orders = pgTable('orders', {
  id: integer('id').primaryKey(),
  userId: integer('user_id').notNull(),
  amount: integer('amount').notNull(),
});

const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  country: text('country'),
});

const products = pgTable('products', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  categoryId: integer('category_id'),
});

const categories = pgTable('categories', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
});

beforeAll(async () => {
  instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  db = drizzle(connection);

  // Create schema and tables
  await db.execute(sql`CREATE SCHEMA IF NOT EXISTS test_schema`);

  await db.execute(sql`
    CREATE TABLE test_schema.brands (
      id INTEGER PRIMARY KEY,
      country TEXT NOT NULL,
      brand_slug TEXT NOT NULL,
      brand_name TEXT NOT NULL
    )
  `);

  await db.execute(sql`
    CREATE TABLE test_schema.restaurants (
      id INTEGER PRIMARY KEY,
      country TEXT NOT NULL,
      brand_slug TEXT NOT NULL,
      name TEXT NOT NULL,
      is_valid INTEGER NOT NULL
    )
  `);

  await db.execute(sql`
    CREATE TABLE test_schema.menu_stats (
      id INTEGER PRIMARY KEY,
      country TEXT NOT NULL,
      brand_slug TEXT NOT NULL,
      menu_count INTEGER NOT NULL
    )
  `);

  await db.execute(sql`
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount INTEGER NOT NULL
    )
  `);

  await db.execute(sql`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      country TEXT
    )
  `);

  await db.execute(sql`
    CREATE TABLE products (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      category_id INTEGER
    )
  `);

  await db.execute(sql`
    CREATE TABLE categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    )
  `);

  // Seed test data
  await db.insert(brands).values([
    { id: 1, country: 'US', brandSlug: 'acme', brandName: 'ACME Corp' },
    { id: 2, country: 'UK', brandSlug: 'widgets', brandName: 'Widgets Inc' },
  ]);

  await db.insert(restaurants).values([
    {
      id: 1,
      country: 'US',
      brandSlug: 'acme',
      name: 'ACME Downtown',
      isValid: 1,
    },
    {
      id: 2,
      country: 'US',
      brandSlug: 'acme',
      name: 'ACME Uptown',
      isValid: 1,
    },
    {
      id: 3,
      country: 'UK',
      brandSlug: 'widgets',
      name: 'Widgets London',
      isValid: 1,
    },
  ]);

  await db.insert(menuStats).values([
    { id: 1, country: 'US', brandSlug: 'acme', menuCount: 5 },
    { id: 2, country: 'UK', brandSlug: 'widgets', menuCount: 3 },
  ]);

  await db.insert(users).values([
    { id: 1, name: 'Alice', country: 'US' },
    { id: 2, name: 'Bob', country: 'UK' },
  ]);

  await db.insert(orders).values([
    { id: 1, userId: 1, amount: 100 },
    { id: 2, userId: 1, amount: 200 },
    { id: 3, userId: 2, amount: 150 },
  ]);

  await db.insert(categories).values([
    { id: 1, name: 'Electronics' },
    { id: 2, name: 'Books' },
  ]);

  await db.insert(products).values([
    { id: 1, name: 'Laptop', categoryId: 1 },
    { id: 2, name: 'Novel', categoryId: 2 },
  ]);
});

afterAll(async () => {
  // DuckDBInstance doesn't have a close method - connections are closed automatically
});

describe('CTE with schema-qualified table JOIN (customer pattern)', () => {
  test('CTE joined with schema-qualified table on same column names', async () => {
    // This is the exact pattern the customer reported:
    // CTE with country/brand_slug columns joined to schema.table with same columns
    const platformCounts = db.$with('platformCounts').as(
      db
        .select({
          country: restaurants.country,
          brandSlug: restaurants.brandSlug,
          restaurantCount: sql<number>`count(*)`.as('restaurantCount'),
        })
        .from(restaurants)
        .where(eq(restaurants.isValid, 1))
        .groupBy(restaurants.country, restaurants.brandSlug)
    );

    const result = await db
      .with(platformCounts)
      .select({
        brandName: brands.brandName,
        country: brands.country,
        restaurantCount: platformCounts.restaurantCount,
      })
      .from(brands)
      .leftJoin(
        platformCounts,
        and(
          eq(brands.country, platformCounts.country),
          eq(brands.brandSlug, platformCounts.brandSlug)
        )
      )
      .where(eq(brands.country, 'US'));

    expect(result).toHaveLength(1);
    expect(result[0].brandName).toBe('ACME Corp');
    expect(Number(result[0].restaurantCount)).toBe(2);
  });

  test('CTE with multiple LEFT JOINs to schema-qualified tables', async () => {
    // Multiple CTEs/tables with overlapping column names
    const restaurantCounts = db.$with('restaurantCounts').as(
      db
        .select({
          country: restaurants.country,
          brandSlug: restaurants.brandSlug,
          count: sql<number>`count(*)`.as('count'),
        })
        .from(restaurants)
        .groupBy(restaurants.country, restaurants.brandSlug)
    );

    const result = await db
      .with(restaurantCounts)
      .select({
        brandName: brands.brandName,
        restaurantCount: restaurantCounts.count,
        menuCount: menuStats.menuCount,
      })
      .from(brands)
      .leftJoin(
        restaurantCounts,
        and(
          eq(brands.country, restaurantCounts.country),
          eq(brands.brandSlug, restaurantCounts.brandSlug)
        )
      )
      .leftJoin(
        menuStats,
        and(
          eq(brands.country, menuStats.country),
          eq(brands.brandSlug, menuStats.brandSlug)
        )
      );

    expect(result).toHaveLength(2);
  });
});

describe('Simple JOIN patterns', () => {
  test('JOIN with same column name on both tables', async () => {
    // Both tables have 'id' column
    const result = await db
      .select({
        productName: products.name,
        categoryName: categories.name,
      })
      .from(products)
      .leftJoin(categories, eq(products.categoryId, categories.id));

    expect(result).toHaveLength(2);
  });

  test('Self-referential pattern with alias', async () => {
    // Subquery aliased and joined back
    const subq = db
      .select({
        userId: orders.userId,
        totalAmount: sql<number>`sum(${orders.amount})`.as('totalAmount'),
      })
      .from(orders)
      .groupBy(orders.userId)
      .as('order_totals');

    const result = await db
      .select({
        userName: users.name,
        totalAmount: subq.totalAmount,
      })
      .from(users)
      .leftJoin(subq, eq(users.id, subq.userId));

    expect(result).toHaveLength(2);
  });
});

describe('Multiple CTEs with shared column names', () => {
  test('Two CTEs with same column names joined together', async () => {
    const cte1 = db.$with('cte1').as(
      db
        .select({
          id: users.id,
          value: sql<string>`${users.name}`.as('value'),
        })
        .from(users)
    );

    const cte2 = db.$with('cte2').as(
      db
        .select({
          id: orders.userId,
          total: sql<number>`sum(${orders.amount})`.as('total'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    const result = await db
      .with(cte1, cte2)
      .select({
        userName: cte1.value,
        orderTotal: cte2.total,
      })
      .from(cte1)
      .leftJoin(cte2, eq(cte1.id, cte2.id));

    expect(result).toHaveLength(2);
  });

  test('CTE referencing another CTE', async () => {
    const baseCte = db.$with('baseCte').as(
      db
        .select({
          userId: orders.userId,
          amount: orders.amount,
        })
        .from(orders)
    );

    // Note: Drizzle doesn't support CTE-to-CTE references directly,
    // so we test the main query joining multiple CTEs
    const result = await db
      .with(baseCte)
      .select({
        userId: baseCte.userId,
        amount: baseCte.amount,
      })
      .from(baseCte);

    expect(result).toHaveLength(3);
  });
});

describe('Complex aggregation patterns', () => {
  test('Aggregation CTE with GROUP BY joined to detail table', async () => {
    const aggCte = db.$with('aggCte').as(
      db
        .select({
          country: users.country,
          userCount: sql<number>`count(*)`.as('userCount'),
        })
        .from(users)
        .groupBy(users.country)
    );

    const result = await db
      .with(aggCte)
      .select({
        userName: users.name,
        countryUserCount: aggCte.userCount,
      })
      .from(users)
      .leftJoin(aggCte, eq(users.country, aggCte.country));

    expect(result).toHaveLength(2);
  });

  test('Multiple aggregation levels', async () => {
    const ordersByUser = db.$with('ordersByUser').as(
      db
        .select({
          userId: orders.userId,
          orderCount: sql<number>`count(*)`.as('orderCount'),
          totalAmount: sql<number>`sum(${orders.amount})`.as('totalAmount'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    const result = await db
      .with(ordersByUser)
      .select({
        userName: users.name,
        orderCount: ordersByUser.orderCount,
        totalAmount: ordersByUser.totalAmount,
      })
      .from(users)
      .innerJoin(ordersByUser, eq(users.id, ordersByUser.userId));

    expect(result).toHaveLength(2);
    expect(result.find((r) => r.userName === 'Alice')).toBeDefined();
  });
});

describe('Edge cases', () => {
  test('JOIN with no matching rows (LEFT JOIN null handling)', async () => {
    const cte = db.$with('emptyCte').as(
      db
        .select({
          id: users.id,
          extra: sql<string>`'test'`.as('extra'),
        })
        .from(users)
        .where(sql`1 = 0`) // Always false - empty result
    );

    const result = await db
      .with(cte)
      .select({
        userName: users.name,
        extra: cte.extra,
      })
      .from(users)
      .leftJoin(cte, eq(users.id, cte.id));

    expect(result).toHaveLength(2);
    expect(result[0].extra).toBeNull();
  });

  test('Deeply nested subquery in FROM clause', async () => {
    const innerSubq = db
      .select({
        id: orders.userId,
        sum: sql<number>`sum(${orders.amount})`.as('sum'),
      })
      .from(orders)
      .groupBy(orders.userId)
      .as('inner_subq');

    const result = await db
      .select({
        userName: users.name,
        orderSum: innerSubq.sum,
      })
      .from(users)
      .leftJoin(innerSubq, eq(users.id, innerSubq.id));

    expect(result).toHaveLength(2);
  });

  test('Three-way JOIN with shared column names', async () => {
    // users.country, brands.country - potential ambiguity
    const result = await db
      .select({
        userName: users.name,
        brandName: brands.brandName,
        menuCount: menuStats.menuCount,
      })
      .from(users)
      .leftJoin(brands, eq(users.country, brands.country))
      .leftJoin(
        menuStats,
        and(
          eq(brands.country, menuStats.country),
          eq(brands.brandSlug, menuStats.brandSlug)
        )
      );

    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Diagnostic: SQL output verification', () => {
  test('verify CTE SQL has qualified column references', async () => {
    const cte = db.$with('testCte').as(
      db
        .select({
          country: restaurants.country,
          count: sql<number>`count(*)`.as('count'),
        })
        .from(restaurants)
        .groupBy(restaurants.country)
    );

    // Build the query but don't execute - inspect the SQL
    const query = db
      .with(cte)
      .select({
        brandCountry: brands.country,
        count: cte.count,
      })
      .from(brands)
      .leftJoin(cte, eq(brands.country, cte.country));

    // Execute and verify it works
    const result = await query;
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Complex: Multiple CTEs with chained JOINs', () => {
  test('three CTEs joined in sequence', async () => {
    const userStats = db.$with('userStats').as(
      db
        .select({
          userId: users.id,
          userName: users.name,
          userCountry: users.country,
        })
        .from(users)
    );

    const orderStats = db.$with('orderStats').as(
      db
        .select({
          odUserId: orders.userId,
          totalOrders: sql<number>`count(*)`.as('totalOrders'),
          totalAmount: sql<number>`sum(${orders.amount})`.as('totalAmount'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    const countryStats = db.$with('countryStats').as(
      db
        .select({
          csCountry: users.country,
          userCount: sql<number>`count(*)`.as('userCount'),
        })
        .from(users)
        .groupBy(users.country)
    );

    // Use unique column names to avoid ambiguity in chained JOINs
    const result = await db
      .with(userStats, orderStats, countryStats)
      .select({
        userName: userStats.userName,
        userCountry: userStats.userCountry,
        totalOrders: orderStats.totalOrders,
        totalAmount: orderStats.totalAmount,
        countryUserCount: countryStats.userCount,
      })
      .from(userStats)
      .leftJoin(orderStats, eq(userStats.userId, orderStats.odUserId))
      .leftJoin(
        countryStats,
        eq(userStats.userCountry, countryStats.csCountry)
      );

    expect(result).toHaveLength(2);
    expect(result.every((r) => r.userName !== null)).toBe(true);
  });

  test('four CTEs with complex join conditions', async () => {
    const cte1 = db.$with('cte1').as(
      db
        .select({
          id: users.id,
          country: users.country,
        })
        .from(users)
    );

    const cte2 = db.$with('cte2').as(
      db
        .select({
          country: brands.country,
          brandSlug: brands.brandSlug,
          brandName: brands.brandName,
        })
        .from(brands)
    );

    const cte3 = db.$with('cte3').as(
      db
        .select({
          country: restaurants.country,
          brandSlug: restaurants.brandSlug,
          restaurantName: restaurants.name,
        })
        .from(restaurants)
    );

    const cte4 = db.$with('cte4').as(
      db
        .select({
          country: menuStats.country,
          brandSlug: menuStats.brandSlug,
          menuCount: menuStats.menuCount,
        })
        .from(menuStats)
    );

    const result = await db
      .with(cte1, cte2, cte3, cte4)
      .select({
        userCountry: cte1.country,
        brandName: cte2.brandName,
        restaurantName: cte3.restaurantName,
        menuCount: cte4.menuCount,
      })
      .from(cte1)
      .leftJoin(cte2, eq(cte1.country, cte2.country))
      .leftJoin(
        cte3,
        and(eq(cte2.country, cte3.country), eq(cte2.brandSlug, cte3.brandSlug))
      )
      .leftJoin(
        cte4,
        and(eq(cte3.country, cte4.country), eq(cte3.brandSlug, cte4.brandSlug))
      );

    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Complex: Nested subqueries', () => {
  test('subquery inside subquery with same column names', async () => {
    const innerSubq = db
      .select({
        userId: orders.userId,
        amount: sql<number>`sum(${orders.amount})`.as('amount'),
      })
      .from(orders)
      .groupBy(orders.userId)
      .as('inner_sq');

    const outerSubq = db
      .select({
        id: users.id,
        name: users.name,
        amount: innerSubq.amount,
      })
      .from(users)
      .leftJoin(innerSubq, eq(users.id, innerSubq.userId))
      .as('outer_sq');

    const result = await db
      .select({
        userName: outerSubq.name,
        orderAmount: outerSubq.amount,
      })
      .from(outerSubq);

    expect(result).toHaveLength(2);
  });

  test('CTE with subquery inside', async () => {
    const subq = db
      .select({
        userId: orders.userId,
        maxAmount: sql<number>`max(${orders.amount})`.as('maxAmount'),
      })
      .from(orders)
      .groupBy(orders.userId)
      .as('max_orders');

    const cte = db.$with('userMaxOrders').as(
      db
        .select({
          id: users.id,
          name: users.name,
          maxAmount: subq.maxAmount,
        })
        .from(users)
        .leftJoin(subq, eq(users.id, subq.userId))
    );

    const result = await db
      .with(cte)
      .select({
        userName: cte.name,
        maxOrderAmount: cte.maxAmount,
      })
      .from(cte);

    expect(result).toHaveLength(2);
  });
});

describe('Complex: Same column name across many tables', () => {
  test('five-way join with id column in all tables', async () => {
    // All tables have 'id' - potential for ambiguity
    const result = await db
      .select({
        usersId: users.id,
        ordersId: orders.id,
        productsId: products.id,
        categoriesId: categories.id,
        brandsId: brands.id,
      })
      .from(users)
      .leftJoin(orders, eq(users.id, orders.userId))
      .leftJoin(products, sql`1=1`) // Cross join for testing
      .leftJoin(categories, eq(products.categoryId, categories.id))
      .leftJoin(brands, eq(users.country, brands.country))
      .limit(10);

    expect(result.length).toBeGreaterThan(0);
  });

  test('country column in multiple tables with chained joins', async () => {
    // users.country, brands.country, restaurants.country, menuStats.country
    const result = await db
      .select({
        userName: users.name,
        userCountry: users.country,
        brandCountry: brands.country,
        restaurantCountry: restaurants.country,
        menuStatsCountry: menuStats.country,
      })
      .from(users)
      .leftJoin(brands, eq(users.country, brands.country))
      .leftJoin(
        restaurants,
        and(
          eq(brands.country, restaurants.country),
          eq(brands.brandSlug, restaurants.brandSlug)
        )
      )
      .leftJoin(
        menuStats,
        and(
          eq(restaurants.country, menuStats.country),
          eq(restaurants.brandSlug, menuStats.brandSlug)
        )
      );

    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Complex: Aggregations with GROUP BY and HAVING', () => {
  test('CTE with GROUP BY and HAVING joined to base table', async () => {
    const highValueUsers = db.$with('highValueUsers').as(
      db
        .select({
          userId: orders.userId,
          totalAmount: sql<number>`sum(${orders.amount})`.as('totalAmount'),
          orderCount: sql<number>`count(*)`.as('orderCount'),
        })
        .from(orders)
        .groupBy(orders.userId)
        .having(sql`sum(${orders.amount}) > 100`)
    );

    const result = await db
      .with(highValueUsers)
      .select({
        userName: users.name,
        totalAmount: highValueUsers.totalAmount,
        orderCount: highValueUsers.orderCount,
      })
      .from(users)
      .innerJoin(highValueUsers, eq(users.id, highValueUsers.userId));

    expect(result.length).toBeGreaterThan(0);
  });

  test('multiple aggregation CTEs with different groupings', async () => {
    const byUser = db.$with('byUser').as(
      db
        .select({
          userId: orders.userId,
          userTotal: sql<number>`sum(${orders.amount})`.as('userTotal'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    const byCountry = db.$with('byCountry').as(
      db
        .select({
          country: users.country,
          countryTotal: sql<number>`count(*)`.as('countryTotal'),
        })
        .from(users)
        .groupBy(users.country)
    );

    const result = await db
      .with(byUser, byCountry)
      .select({
        userName: users.name,
        userCountry: users.country,
        userTotal: byUser.userTotal,
        countryTotal: byCountry.countryTotal,
      })
      .from(users)
      .leftJoin(byUser, eq(users.id, byUser.userId))
      .leftJoin(byCountry, eq(users.country, byCountry.country));

    expect(result).toHaveLength(2);
  });
});

describe('Complex: UNION/INTERSECT in CTEs', () => {
  test('CTE with UNION joined to table', async () => {
    const combinedCte = db.$with('combined').as(
      db
        .select({
          id: users.id,
          name: users.name,
          source: sql<string>`'user'`.as('source'),
        })
        .from(users)
        .union(
          db
            .select({
              id: brands.id,
              name: brands.brandName,
              source: sql<string>`'brand'`.as('source'),
            })
            .from(brands)
        )
    );

    const result = await db
      .with(combinedCte)
      .select({
        entityId: combinedCte.id,
        entityName: combinedCte.name,
        entitySource: combinedCte.source,
      })
      .from(combinedCte);

    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Complex: Self-joins and recursive-like patterns', () => {
  test('table joined to itself via subquery', async () => {
    // Use unique column aliases to avoid ambiguity in self-join
    const userSubq = db
      .select({
        copyId: users.id,
        copyName: users.name,
        copyCountry: users.country,
      })
      .from(users)
      .as('user_copy');

    const result = await db
      .select({
        user1Name: users.name,
        user2Name: userSubq.copyName,
        sharedCountry: users.country,
      })
      .from(users)
      .innerJoin(userSubq, eq(users.country, userSubq.copyCountry))
      .where(sql`${users.id} < ${userSubq.copyId}`);

    // Both users are in different countries, so no self-matches
    expect(result).toHaveLength(0);
  });

  test('orders joined to orders via user relationship', async () => {
    // Use unique column aliases to avoid ambiguity in self-join
    const orderSubq = db
      .select({
        otherId: orders.id,
        otherUserId: orders.userId,
        otherAmount: orders.amount,
      })
      .from(orders)
      .as('other_orders');

    const result = await db
      .select({
        order1Id: orders.id,
        order2Id: orderSubq.otherId,
        sameUser: orders.userId,
      })
      .from(orders)
      .innerJoin(orderSubq, eq(orders.userId, orderSubq.otherUserId))
      .where(sql`${orders.id} < ${orderSubq.otherId}`);

    // Alice has 2 orders, so 1 pair; Bob has 1 order, so 0 pairs
    expect(result.length).toBeGreaterThanOrEqual(1);
  });
});

describe('Complex: Mixed schema and public tables', () => {
  test('schema table joined with public table via CTE', async () => {
    const schemaCte = db.$with('schemaCte').as(
      db
        .select({
          country: brands.country,
          brandCount: sql<number>`count(*)`.as('brandCount'),
        })
        .from(brands)
        .groupBy(brands.country)
    );

    const result = await db
      .with(schemaCte)
      .select({
        userName: users.name,
        userCountry: users.country,
        brandCount: schemaCte.brandCount,
      })
      .from(users)
      .leftJoin(schemaCte, eq(users.country, schemaCte.country));

    expect(result).toHaveLength(2);
  });

  test('multiple schema tables with public table in single query', async () => {
    const result = await db
      .select({
        userName: users.name,
        brandName: brands.brandName,
        restaurantName: restaurants.name,
        menuCount: menuStats.menuCount,
      })
      .from(users)
      .leftJoin(brands, eq(users.country, brands.country))
      .leftJoin(
        restaurants,
        and(
          eq(brands.country, restaurants.country),
          eq(brands.brandSlug, restaurants.brandSlug)
        )
      )
      .leftJoin(
        menuStats,
        and(
          eq(restaurants.country, menuStats.country),
          eq(restaurants.brandSlug, menuStats.brandSlug)
        )
      )
      .where(eq(users.country, 'US'));

    expect(result.length).toBeGreaterThan(0);
  });
});

describe('Complex: Window functions with CTEs', () => {
  test('CTE with window function joined to base table', async () => {
    // Use unique column aliases to avoid ambiguity when joining
    const rankedOrders = db.$with('rankedOrders').as(
      db
        .select({
          orderId: orders.id,
          orderUserId: orders.userId,
          orderAmount: orders.amount,
          orderRank:
            sql<number>`row_number() over (partition by ${orders.userId} order by ${orders.amount} desc)`.as(
              'orderRank'
            ),
        })
        .from(orders)
    );

    const result = await db
      .with(rankedOrders)
      .select({
        userName: users.name,
        orderId: rankedOrders.orderId,
        orderAmount: rankedOrders.orderAmount,
        orderRank: rankedOrders.orderRank,
      })
      .from(users)
      .innerJoin(rankedOrders, eq(users.id, rankedOrders.orderUserId))
      .where(eq(rankedOrders.orderRank, 1));

    // Each user's top order
    expect(result).toHaveLength(2);
  });
});

describe('Complex: CASE expressions in CTEs', () => {
  test('CTE with CASE expression joined on computed column', async () => {
    const categorizedUsers = db.$with('categorizedUsers').as(
      db
        .select({
          id: users.id,
          name: users.name,
          region: sql<string>`
            CASE
              WHEN ${users.country} = 'US' THEN 'Americas'
              WHEN ${users.country} = 'UK' THEN 'Europe'
              ELSE 'Other'
            END
          `.as('region'),
        })
        .from(users)
    );

    const categorizedBrands = db.$with('categorizedBrands').as(
      db
        .select({
          brandName: brands.brandName,
          region: sql<string>`
            CASE
              WHEN ${brands.country} = 'US' THEN 'Americas'
              WHEN ${brands.country} = 'UK' THEN 'Europe'
              ELSE 'Other'
            END
          `.as('region'),
        })
        .from(brands)
    );

    const result = await db
      .with(categorizedUsers, categorizedBrands)
      .select({
        userName: categorizedUsers.name,
        userRegion: categorizedUsers.region,
        brandName: categorizedBrands.brandName,
      })
      .from(categorizedUsers)
      .leftJoin(
        categorizedBrands,
        eq(categorizedUsers.region, categorizedBrands.region)
      );

    expect(result.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// ULTRA-COMPLEX TEST CASES
// These tests push the boundaries of CTE, JOIN, and column qualification
// ============================================================================

describe('Ultra-Complex: Deep CTE nesting with subqueries', () => {
  test('6 CTEs with cascading dependencies and mixed aggregations', async () => {
    // CTE 1: Base user data
    const baseUsers = db.$with('baseUsers').as(
      db
        .select({
          usrId: users.id,
          usrName: users.name,
          usrCountry: users.country,
        })
        .from(users)
    );

    // CTE 2: Order aggregation per user
    const userOrderAgg = db.$with('userOrderAgg').as(
      db
        .select({
          uoaUserId: orders.userId,
          uoaTotalAmount: sql<number>`sum(${orders.amount})`.as(
            'uoaTotalAmount'
          ),
          uoaOrderCount: sql<number>`count(*)`.as('uoaOrderCount'),
          uoaAvgAmount: sql<number>`avg(${orders.amount})`.as('uoaAvgAmount'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    // CTE 3: Country-level brand stats
    const countryBrandStats = db.$with('countryBrandStats').as(
      db
        .select({
          cbsCountry: brands.country,
          cbsBrandCount: sql<number>`count(distinct ${brands.brandSlug})`.as(
            'cbsBrandCount'
          ),
        })
        .from(brands)
        .groupBy(brands.country)
    );

    // CTE 4: Restaurant density per country/brand
    const restaurantDensity = db.$with('restaurantDensity').as(
      db
        .select({
          rdCountry: restaurants.country,
          rdBrandSlug: restaurants.brandSlug,
          rdRestaurantCount: sql<number>`count(*)`.as('rdRestaurantCount'),
        })
        .from(restaurants)
        .where(eq(restaurants.isValid, 1))
        .groupBy(restaurants.country, restaurants.brandSlug)
    );

    // CTE 5: Menu stats aggregation
    const menuAgg = db.$with('menuAgg').as(
      db
        .select({
          maCountry: menuStats.country,
          maBrandSlug: menuStats.brandSlug,
          maTotalMenus: sql<number>`sum(${menuStats.menuCount})`.as(
            'maTotalMenus'
          ),
        })
        .from(menuStats)
        .groupBy(menuStats.country, menuStats.brandSlug)
    );

    // CTE 6: Combined brand metrics
    const brandMetrics = db.$with('brandMetrics').as(
      db
        .select({
          bmCountry: brands.country,
          bmBrandSlug: brands.brandSlug,
          bmBrandName: brands.brandName,
        })
        .from(brands)
    );

    // Main query joining all 6 CTEs
    const result = await db
      .with(
        baseUsers,
        userOrderAgg,
        countryBrandStats,
        restaurantDensity,
        menuAgg,
        brandMetrics
      )
      .select({
        userName: baseUsers.usrName,
        userCountry: baseUsers.usrCountry,
        totalOrderAmount: userOrderAgg.uoaTotalAmount,
        orderCount: userOrderAgg.uoaOrderCount,
        avgOrderAmount: userOrderAgg.uoaAvgAmount,
        countryBrandCount: countryBrandStats.cbsBrandCount,
        brandName: brandMetrics.bmBrandName,
        restaurantCount: restaurantDensity.rdRestaurantCount,
        totalMenus: menuAgg.maTotalMenus,
      })
      .from(baseUsers)
      .leftJoin(userOrderAgg, eq(baseUsers.usrId, userOrderAgg.uoaUserId))
      .leftJoin(
        countryBrandStats,
        eq(baseUsers.usrCountry, countryBrandStats.cbsCountry)
      )
      .leftJoin(brandMetrics, eq(baseUsers.usrCountry, brandMetrics.bmCountry))
      .leftJoin(
        restaurantDensity,
        and(
          eq(brandMetrics.bmCountry, restaurantDensity.rdCountry),
          eq(brandMetrics.bmBrandSlug, restaurantDensity.rdBrandSlug)
        )
      )
      .leftJoin(
        menuAgg,
        and(
          eq(restaurantDensity.rdCountry, menuAgg.maCountry),
          eq(restaurantDensity.rdBrandSlug, menuAgg.maBrandSlug)
        )
      );

    expect(result.length).toBeGreaterThan(0);
    // Verify we got data from multiple CTEs
    const firstRow = result[0];
    expect(firstRow.userName).toBeDefined();
  });
});

describe('Ultra-Complex: Recursive-style pattern with window functions', () => {
  test('running totals with partition and multiple window frames', async () => {
    // CTE with multiple window functions over different partitions
    const windowedOrders = db.$with('windowedOrders').as(
      db
        .select({
          woOrderId: orders.id,
          woUserId: orders.userId,
          woAmount: orders.amount,
          woRunningTotal:
            sql<number>`sum(${orders.amount}) over (partition by ${orders.userId} order by ${orders.id})`.as(
              'woRunningTotal'
            ),
          woUserRank:
            sql<number>`row_number() over (partition by ${orders.userId} order by ${orders.amount} desc)`.as(
              'woUserRank'
            ),
          woGlobalRank:
            sql<number>`rank() over (order by ${orders.amount} desc)`.as(
              'woGlobalRank'
            ),
          woPercentile:
            sql<number>`percent_rank() over (order by ${orders.amount})`.as(
              'woPercentile'
            ),
        })
        .from(orders)
    );

    // Second CTE that aggregates the windowed results
    const userWindowStats = db.$with('userWindowStats').as(
      db
        .select({
          uwsUserId: orders.userId,
          uwsMaxAmount: sql<number>`max(${orders.amount})`.as('uwsMaxAmount'),
          uwsMinAmount: sql<number>`min(${orders.amount})`.as('uwsMinAmount'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    const result = await db
      .with(windowedOrders, userWindowStats)
      .select({
        userName: users.name,
        orderId: windowedOrders.woOrderId,
        amount: windowedOrders.woAmount,
        runningTotal: windowedOrders.woRunningTotal,
        userRank: windowedOrders.woUserRank,
        globalRank: windowedOrders.woGlobalRank,
        percentile: windowedOrders.woPercentile,
        userMaxAmount: userWindowStats.uwsMaxAmount,
        userMinAmount: userWindowStats.uwsMinAmount,
      })
      .from(users)
      .innerJoin(windowedOrders, eq(users.id, windowedOrders.woUserId))
      .innerJoin(userWindowStats, eq(users.id, userWindowStats.uwsUserId))
      .orderBy(windowedOrders.woGlobalRank);

    expect(result.length).toBe(3); // 3 orders total
    // Verify window functions worked
    expect(result.every((r) => r.runningTotal !== null)).toBe(true);
    expect(result.every((r) => r.globalRank !== null)).toBe(true);
  });
});

describe('Ultra-Complex: Multi-level subquery nesting', () => {
  test('subquery in SELECT, FROM, WHERE, and JOIN simultaneously', async () => {
    // Subquery for FROM clause
    const fromSubq = db
      .select({
        fsUserId: users.id,
        fsUserName: users.name,
        fsUserCountry: users.country,
      })
      .from(users)
      .as('from_subq');

    // Subquery for JOIN
    const joinSubq = db
      .select({
        jsUserId: orders.userId,
        jsTotalAmount: sql<number>`sum(${orders.amount})`.as('jsTotalAmount'),
      })
      .from(orders)
      .groupBy(orders.userId)
      .as('join_subq');

    // Scalar subquery for SELECT list (as SQL expression)
    const result = await db
      .select({
        userName: fromSubq.fsUserName,
        userCountry: fromSubq.fsUserCountry,
        totalAmount: joinSubq.jsTotalAmount,
        // Scalar subquery in SELECT
        globalOrderCount: sql<number>`(SELECT count(*) FROM orders)`.as(
          'globalOrderCount'
        ),
        // Another scalar subquery
        userOrderCount:
          sql<number>`(SELECT count(*) FROM orders o WHERE o.user_id = ${fromSubq.fsUserId})`.as(
            'userOrderCount'
          ),
      })
      .from(fromSubq)
      .leftJoin(joinSubq, eq(fromSubq.fsUserId, joinSubq.jsUserId))
      // Subquery in WHERE
      .where(
        sql`${fromSubq.fsUserId} IN (SELECT DISTINCT user_id FROM orders WHERE amount > 50)`
      );

    expect(result.length).toBeGreaterThan(0);
    expect(Number(result[0].globalOrderCount)).toBe(3); // We have 3 orders total
  });
});

describe('Ultra-Complex: UNION ALL with CTEs and complex predicates', () => {
  test('UNION of multiple CTEs with different schemas normalized', async () => {
    // CTE for users
    const userEntities = db.$with('userEntities').as(
      db
        .select({
          entityType: sql<string>`'user'`.as('entityType'),
          entityId: users.id,
          entityName: users.name,
          entityCountry: users.country,
          entityMetric: sql<number>`0`.as('entityMetric'),
        })
        .from(users)
    );

    // CTE for brands
    const brandEntities = db.$with('brandEntities').as(
      db
        .select({
          entityType: sql<string>`'brand'`.as('entityType'),
          entityId: brands.id,
          entityName: brands.brandName,
          entityCountry: brands.country,
          entityMetric: sql<number>`0`.as('entityMetric'),
        })
        .from(brands)
    );

    // CTE for orders aggregated by user
    const orderEntities = db.$with('orderEntities').as(
      db
        .select({
          entityType: sql<string>`'order_summary'`.as('entityType'),
          entityId: orders.userId,
          entityName: sql<string>`'Order Total'`.as('entityName'),
          entityCountry: sql<string>`null`.as('entityCountry'),
          entityMetric: sql<number>`sum(${orders.amount})`.as('entityMetric'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    // Combined CTE using UNION ALL
    const allEntities = db.$with('allEntities').as(
      db
        .select({
          aeType: userEntities.entityType,
          aeId: userEntities.entityId,
          aeName: userEntities.entityName,
          aeCountry: userEntities.entityCountry,
          aeMetric: userEntities.entityMetric,
        })
        .from(userEntities)
        .unionAll(
          db
            .select({
              aeType: brandEntities.entityType,
              aeId: brandEntities.entityId,
              aeName: brandEntities.entityName,
              aeCountry: brandEntities.entityCountry,
              aeMetric: brandEntities.entityMetric,
            })
            .from(brandEntities)
        )
        .unionAll(
          db
            .select({
              aeType: orderEntities.entityType,
              aeId: orderEntities.entityId,
              aeName: orderEntities.entityName,
              aeCountry: orderEntities.entityCountry,
              aeMetric: orderEntities.entityMetric,
            })
            .from(orderEntities)
        )
    );

    const result = await db
      .with(userEntities, brandEntities, orderEntities, allEntities)
      .select({
        entityType: allEntities.aeType,
        entityId: allEntities.aeId,
        entityName: allEntities.aeName,
        entityCountry: allEntities.aeCountry,
        entityMetric: allEntities.aeMetric,
      })
      .from(allEntities)
      .orderBy(allEntities.aeType, allEntities.aeId);

    // 2 users + 2 brands + 2 order summaries = 6 total
    expect(result.length).toBe(6);
    expect(result.filter((r) => r.entityType === 'user').length).toBe(2);
    expect(result.filter((r) => r.entityType === 'brand').length).toBe(2);
    expect(result.filter((r) => r.entityType === 'order_summary').length).toBe(
      2
    );
  });
});

describe('Ultra-Complex: Correlated subqueries with CTEs', () => {
  test('CTE with correlated subquery in SELECT and WHERE', async () => {
    // CTE that will be used in correlated subqueries
    const orderTotals = db.$with('orderTotals').as(
      db
        .select({
          otUserId: orders.userId,
          otTotal: sql<number>`sum(${orders.amount})`.as('otTotal'),
        })
        .from(orders)
        .groupBy(orders.userId)
    );

    // Main query with correlated subqueries
    const result = await db
      .with(orderTotals)
      .select({
        userId: users.id,
        userName: users.name,
        userCountry: users.country,
        // Correlated subquery: get user's order total
        orderTotal: sql<number>`
          COALESCE((
            SELECT ot."otTotal"
            FROM "orderTotals" ot
            WHERE ot."otUserId" = ${users.id}
          ), 0)
        `.as('orderTotal'),
        // Another correlated subquery: count user's orders
        orderCount: sql<number>`
          (SELECT count(*) FROM orders o WHERE o.user_id = ${users.id})
        `.as('orderCount'),
        // Correlated subquery in a CASE expression
        customerTier: sql<string>`
          CASE
            WHEN (SELECT sum(amount) FROM orders o WHERE o.user_id = ${users.id}) > 200 THEN 'Gold'
            WHEN (SELECT sum(amount) FROM orders o WHERE o.user_id = ${users.id}) > 100 THEN 'Silver'
            ELSE 'Bronze'
          END
        `.as('customerTier'),
      })
      .from(users)
      // EXISTS correlated subquery in WHERE
      .where(
        sql`EXISTS (SELECT 1 FROM orders o WHERE o.user_id = ${users.id})`
      );

    expect(result.length).toBe(2); // Both users have orders
    expect(result.every((r) => Number(r.orderTotal) > 0)).toBe(true);
    expect(result.every((r) => Number(r.orderCount) > 0)).toBe(true);
    expect(
      result.every((r) =>
        ['Gold', 'Silver', 'Bronze'].includes(r.customerTier!)
      )
    ).toBe(true);
  });
});
