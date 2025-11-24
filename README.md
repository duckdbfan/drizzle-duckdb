# drizzle-neo-duckdb

DuckDB dialect glue for [drizzle-orm](https://orm.drizzle.team/), built on the Postgres driver surface but targeting DuckDB. Published from the `@leonardovida-md` scope with a focus on getting Drizzle’s query builder, migrations, and type inference working against DuckDB’s Node runtime.

- **Runtime target:** `@duckdb/node-api@1.4.2-r.1` (DuckDB Node API only).
- **Module format:** ESM only, `moduleResolution: bundler`, explicit `.ts` extensions in source.
- **Status:** Experimental; feature coverage is still growing and several DuckDB-specific types/behaviors are missing (see below).

## Installation

```sh
bun add @leonardovida-md/drizzle-neo-duckdb @duckdb/node-api@1.4.2-r.1
```

## Quick start (Node API)

```ts
import { DuckDBInstance } from '@duckdb/node-api';
import { drizzle } from '@leonardovida-md/drizzle-neo-duckdb';
import { DefaultLogger, sql } from 'drizzle-orm';
import { char, integer, pgSchema, text } from 'drizzle-orm/pg-core';

const instance = await DuckDBInstance.create(':memory:');
const connection = await instance.connect();
const db = drizzle(connection, { logger: new DefaultLogger() });

const customSchema = pgSchema('custom');

await db.execute(sql`CREATE SCHEMA IF NOT EXISTS ${customSchema}`);
await db.execute(sql`CREATE SEQUENCE IF NOT EXISTS serial_cities;`);

const cities = customSchema.table('cities', {
  id: integer('id').primaryKey().default(sql`nextval('serial_cities')`),
  name: text('name').notNull(),
  state: char('state', { length: 2 }),
});

await db.execute(sql`
  create table if not exists ${cities} (
    id integer primary key default nextval('serial_cities'),
    name text not null,
    state char(2)
  )
`);

const inserted = await db
  .insert(cities)
  .values([
    { name: 'Paris', state: 'FR' },
    { name: 'London', state: 'UK' },
  ])
  .returning({ id: cities.id });

console.log(inserted);
connection.closeSync();
```

## Migrations

Use `migrate(db, './path/to/migrations')` (or pass the full `MigrationConfig`) to apply SQL files. Migration metadata lives in the `drizzle.__drizzle_migrations` table by default with a schema-local sequence named `__drizzle_migrations_id_seq`; custom `migrationsSchema`/`migrationsTable` values get their own scoped sequence as well.

## Introspection (DuckDB)

- Generate schema code straight from DuckDB: `bun x duckdb-introspect --url ':memory:' --schema my_schema --out ./drizzle/schema.ts`.
- Defaults target DuckDB helpers (e.g., `duckDbTimestamp`, `duckDbJson`, `duckDbStruct`, `duckDbList`) and avoid Postgres JSON/JSONB.
- Pass `--use-pg-time` to emit pg-core `timestamp/date/time` builders instead of the DuckDB-specific variants.
- MotherDuck URLs (`md:`) pick up `MOTHERDUCK_TOKEN` automatically when present; views stay excluded unless you pass `--include-views`.

## Custom column helpers (experimental)

The package ships a few DuckDB-oriented helpers in `columns.ts`:

- `duckDbStruct`, `duckDbMap` for `STRUCT`/`MAP`.
- `duckDbList`, `duckDbArray` for DuckDB list/array columns (uses native list semantics).
- `duckDbTimestamp`, `duckDbDate`, `duckDbTime` for timestamp/date/time handling.
- `duckDbBlob`, `duckDbInet`, `duckDbInterval` for binary, inet, and interval support.
- `duckDbArrayContains/Contained/Overlaps` expression helpers backed by DuckDB’s `array_has_*` functions.

They rely on DuckDB-native literals rather than Postgres operators. If you want to avoid Postgres array operators (`@>`, `<@`, `&&`), import these helpers from `@leonardovida-md/drizzle-neo-duckdb`.

## Known gaps and behavior differences

This connector is not “full fidelity” with Drizzle’s Postgres driver. Notable partial areas:

- **Date/time handling:** The full timestamp/time/date mode matrix (`string` vs `date`, with/without timezone, precision) still follows DuckDB defaults, but offset strings now normalize correctly in both string and Date modes.
- **Result aliasing:** Node API results keep duplicate column aliases in order and suffix repeats to avoid collisions during deep selections.
- **Transactions:** Nested `transaction` calls reuse the outer transaction context (DuckDB doesn’t support `SAVEPOINT`), so inner rollbacks abort the whole transaction. JSON/JSONB columns stay unsupported.
- **Runtime ergonomics:** No statement caching/streaming; results are materialized as objects. Map/struct helpers keep minimal validation.

The suite under `test/` documents the remaining divergences; contributions to close them are welcome.

## Developing

- Install: `bun install`
- Run tests: `bun test`
- Build bundles and types: `bun run build` (emits `dist/index.mjs` and `dist/index.d.ts`)
- Publish to npm: `bun run build` then `npm publish` (ESM-only entry point via `exports` map)

Source lives in `src/*.ts`; generated artifacts in `dist/` should never be edited by hand.

## Contributing

Pull requests are welcome. Please include:

- A short, imperative commit/PR title.
- Failing/repro tests where possible (`test/<feature>.test.ts` is preferred over modifying the big `duckdb.test.ts` unless necessary).
- Notes on DuckDB-specific quirks or limitations you encountered.

## Examples

- A minimal MotherDuck + Drizzle script that reads the built-in `sample_database.nyc.taxi` share lives in `example/motherduck-nyc.ts`. Set `MOTHERDUCK_TOKEN` and run `bun example/motherduck-nyc.ts` (see `example/README.md` for details).

## License

Apache-2.0
