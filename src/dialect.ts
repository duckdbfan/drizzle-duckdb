import { entityKind, is } from 'drizzle-orm/entity';
import type { MigrationConfig, MigrationMeta } from 'drizzle-orm/migrator';
import {
  PgDate,
  PgDateString,
  PgDialect,
  PgJson,
  PgJsonb,
  PgNumeric,
  PgSession,
  PgTime,
  PgTimestamp,
  PgTimestampString,
  PgUUID,
} from 'drizzle-orm/pg-core';
import { DuckDBSession } from './session';
import { sql, type DriverValueEncoder, type QueryTypingsValue } from 'drizzle-orm';

export class DuckDBDialect extends PgDialect {
  static readonly [entityKind]: string = 'DuckDBPgDialect';

  override async migrate(
    migrations: MigrationMeta[],
    session: PgSession,
    config: string | MigrationConfig
  ): Promise<void> {
    const migrationsSchema = typeof config === 'string' ? 'drizzle' : config.migrationsSchema ?? 'drizzle';

    const migrationsTable =
      typeof config === 'string' ? '__drizzle_migrations' : config.migrationsTable ?? '__drizzle_migrations';

    const migrationTableCreate = sql`
			CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)} (
				id integer PRIMARY KEY default nextval('migrations_pk_seq'),
				hash text NOT NULL,
				created_at bigint
			)
		`;

    await session.execute(sql.raw('CREATE SEQUENCE IF NOT EXISTS migrations_pk_seq'));
    await session.execute(sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(migrationsSchema)}`);
    await session.execute(migrationTableCreate);

    const dbMigrations = await session.all<{ id: number; hash: string; created_at: string }>(
      sql`select id, hash, created_at from ${sql.identifier(migrationsSchema)}.${sql.identifier(
        migrationsTable
      )} order by created_at desc limit 1`
    );

    const lastDbMigration = dbMigrations[0];

    await session.transaction(async (tx) => {
      for await (const migration of migrations) {
        if (!lastDbMigration || Number(lastDbMigration.created_at) < migration.folderMillis) {
          for (const stmt of migration.sql) {
            await tx.execute(sql.raw(stmt));
          }

          await tx.execute(
            sql`insert into ${sql.identifier(migrationsSchema)}.${sql.identifier(
              migrationsTable
            )} ("hash", "created_at") values(${migration.hash}, ${migration.folderMillis})`
          );
        }
      }
    });
  }

  override prepareTyping(encoder: DriverValueEncoder<unknown, unknown>): QueryTypingsValue {
    if (is(encoder, PgJsonb) || is(encoder, PgJson)) {
      throw new Error('JSON and JSONB types are not supported in DuckDB');
    } else if (is(encoder, PgNumeric)) {
      return 'decimal';
    } else if (is(encoder, PgTime)) {
      return 'time';
    } else if (is(encoder, PgTimestamp) || is(encoder, PgTimestampString)) {
      return 'timestamp';
    } else if (is(encoder, PgDate) || is(encoder, PgDateString)) {
      return 'date';
    } else if (is(encoder, PgUUID)) {
      return 'uuid';
    } else {
      return 'none';
    }
  }
}
