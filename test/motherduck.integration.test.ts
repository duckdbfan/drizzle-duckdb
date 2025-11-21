import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';
import { sql } from 'drizzle-orm';
import { doublePrecision, integer, pgTable, timestamp } from 'drizzle-orm/pg-core';
import { drizzle } from '../src';
import { expect, test } from 'vitest';

const motherduckToken = process.env.MOTHERDUCK_TOKEN;

if (!motherduckToken) {
  test.skip('MotherDuck integration requires MOTHERDUCK_TOKEN');
} else {
  test(
    'runs the MotherDuck nyc.taxi example against sample_data',
    async () => {
      const instance = await DuckDBInstance.create('md:', { motherduck_token: motherduckToken });
      const connection: DuckDBConnection = await instance.connect();
      const db = drizzle(connection);

      try {
        await db.execute(sql`
          create or replace temp view taxi_sample as
          select
            vendorid,
            tpep_pickup_datetime,
            passenger_count,
            trip_distance,
            total_amount,
            tip_amount
          from sample_data.nyc.taxi
          limit 50000
        `);

        const taxiSample = pgTable('taxi_sample', {
          vendorId: integer('vendorid'),
          pickupTime: timestamp('tpep_pickup_datetime', { withTimezone: false }),
          passengerCount: integer('passenger_count'),
          tripDistance: doublePrecision('trip_distance'),
          totalAmount: doublePrecision('total_amount'),
          tipAmount: doublePrecision('tip_amount'),
        });

        const trips = await db
          .select({
            pickupTime: taxiSample.pickupTime,
            passengerCount: taxiSample.passengerCount,
            tripDistance: taxiSample.tripDistance,
            totalAmount: taxiSample.totalAmount,
            tipAmount: taxiSample.tipAmount,
          })
          .from(taxiSample)
          .limit(5);

        expect(trips.length).toBeGreaterThan(0);
        trips.forEach((trip) => {
          expect(trip.pickupTime).toBeInstanceOf(Date);
          expect(typeof trip.tripDistance).toBe('number');
          expect(typeof trip.totalAmount).toBe('number');
        });

        const tipByPassengers = await db
          .select({
            passengers: taxiSample.passengerCount,
            avgFare: sql<number>`avg(${taxiSample.totalAmount})`,
            avgTip: sql<number>`avg(${taxiSample.tipAmount})`,
          })
          .from(taxiSample)
          .groupBy(taxiSample.passengerCount)
          .orderBy(sql`avg(${taxiSample.tipAmount}) desc`)
          .limit(5);

        expect(tipByPassengers.length).toBeGreaterThan(0);
        expect(Number(tipByPassengers[0].avgTip)).toBeGreaterThan(0);
        for (let i = 1; i < tipByPassengers.length; i++) {
          expect(Number(tipByPassengers[i - 1].avgTip)).toBeGreaterThanOrEqual(
            Number(tipByPassengers[i].avgTip)
          );
        }
      } finally {
        connection.closeSync();
        instance.closeSync();
      }
    },
    120_000
  );
}
