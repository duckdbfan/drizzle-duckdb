# drizzle-duckdb

## Description
A drizzle ORM client for use with DuckDB. Based on drizzle's Postgres client. As of writing this, certain things will work, and others won't. Notably, DuckDB-specific column types such as `struct`, `list`, `array` are not implemented, but this could be done using [drizzle custom types](https://orm.drizzle.team/docs/custom-types). (This is planned to be implemented in the package later on)

## Disclaimers
- **Experimental**: This project is in an experimental stage. Certain features may be broken or not function as expected.
- **Use at Your Own Risk**: Users should proceed with caution and use this project at their own risk.
- **Maintenance**: This project may not be actively maintained. Updates and bug fixes are not guaranteed.

## Getting Started
1. Install dependencies:
    ```sh
    bun add @duckdbfan/drizzle-duckdb
    ```
2. Figure it out! (sorry, might flesh this out later- see tests for some examples)
    ```typescript
    import { Database } from 'duckdb-async';
    import { drizzle } from '@duckdbfan/drizzle-duckdb';
    import { DefaultLogger, sql } from 'drizzle-orm';
    import { char, integer, pgSchema, text } from 'drizzle-orm/pg-core';
    
    const client = await Database.create(':memory:');
    const db = drizzle(client, { logger: new DefaultLogger() });

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

    ```

## Contributing
Contributions are welcome, although I may not be very responsive.

## License
This project is licensed under the Apache License.