# drizzle-neo-duckdb

## Description
A drizzle ORM client for use with DuckDB. Based on drizzle's Postgres client. As of writing this, certain things will work, and others won't. Notably, DuckDB-specific column types such as `struct`, `list`, `array` are not implemented, but this could be done using [drizzle custom types](https://orm.drizzle.team/docs/custom-types). (This is planned to be implemented in the package later on)

## Disclaimers
- **Experimental**: This project is in an experimental stage. Certain features may be broken or not function as expected.
- **Use at Your Own Risk**: Users should proceed with caution and use this project at their own risk.
- **Maintenance**: This project may not be actively maintained. Updates and bug fixes are not guaranteed.

## Getting Started
1. Install dependencies:
    ```sh
    bun add @duckdbfan/drizzle-neo-duckdb @duckdb/node-api@1.4.2-r.1
    ```
2. Figure it out! (sorry, might flesh this out later- see tests for some examples)
    ```typescript
    import { DuckDBInstance } from '@duckdb/node-api';
    import { drizzle } from '@duckdbfan/drizzle-neo-duckdb';
    import { DefaultLogger, sql } from 'drizzle-orm';
    import { char, integer, pgSchema, text } from 'drizzle-orm/pg-core';
    
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    const db = drizzle(connection, { logger: new DefaultLogger() });

    const customSchema = pgSchema('custom');

    await db.execute(sql`CREATE SCHEMA IF NOT EXISTS ${customSchema}`);

    const citiesTable = customSchema.table('cities', {
      id: integer('id')
        .primaryKey()
        .default(sql`nextval('serial_cities')`),
      name: text('name').notNull(),
      state: char('state', { length: 2 }),
    });

    await db.execute(sql`CREATE SEQUENCE IF NOT EXISTS serial_cities;`);

    await db.execute(
      sql`
        create table if not exists ${citiesTable} (
          id integer primary key default nextval('serial_cities'),
          name text not null,
          state char(2)
        )
      `
    );

    const insertedIds = await db
      .insert(citiesTable)
      .values([
        { name: 'Paris', state: 'FR' },
        { name: 'London', state: 'UK' },
      ])
      .returning({ id: citiesTable.id });

    console.log(insertedIds);
    
    connection.closeSync();
    ```

## Using the DuckDB Node API
The recommended runtime client is [`@duckdb/node-api@1.4.2-r.1`](https://www.npmjs.com/package/@duckdb/node-api), which this package now pins in `peerDependencies` to avoid unexpected binary changes. The driver still supports the legacy `duckdb-async` wrapper if you install it separately, but the tests and docs use the Node API connection by default.

## Contributing
Contributions are welcome, although I may not be very responsive.

## License
This project is licensed under the Apache License.
