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
import {
  sql,
  type DriverValueEncoder,
  type QueryTypingsValue,
} from 'drizzle-orm';

export class DuckDBDialect extends PgDialect {
  static readonly [entityKind]: string = 'DuckDBPgDialect';
  private hasPgJsonColumn = false;

  assertNoPgJsonColumns(): void {
    if (this.hasPgJsonColumn) {
      throw new Error(
        'Pg JSON/JSONB columns are not supported in DuckDB. Replace them with duckDbJson() to use DuckDB’s native JSON type.'
      );
    }
  }

  override async migrate(
    migrations: MigrationMeta[],
    session: PgSession,
    config: MigrationConfig | string
  ): Promise<void> {
    const migrationConfig: MigrationConfig =
      typeof config === 'string' ? { migrationsFolder: config } : config;

    const migrationsSchema = migrationConfig.migrationsSchema ?? 'drizzle';
    const migrationsTable =
      migrationConfig.migrationsTable ?? '__drizzle_migrations';
    const migrationsSequence = `${migrationsTable}_id_seq`;
    const legacySequence = 'migrations_pk_seq';

    const escapeIdentifier = (value: string) => value.replace(/"/g, '""');
    const sequenceLiteral = `"${escapeIdentifier(
      migrationsSchema
    )}"."${escapeIdentifier(migrationsSequence)}"`;

    const migrationTableCreate = sql`
      CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(
        migrationsTable
      )} (
        id integer PRIMARY KEY default nextval('${sql.raw(sequenceLiteral)}'),
        hash text NOT NULL,
        created_at bigint
      )
    `;

    await session.execute(
      sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(migrationsSchema)}`
    );
    await session.execute(
      sql`CREATE SEQUENCE IF NOT EXISTS ${sql.identifier(
        migrationsSchema
      )}.${sql.identifier(migrationsSequence)}`
    );
    if (legacySequence !== migrationsSequence) {
      await session.execute(
        sql`CREATE SEQUENCE IF NOT EXISTS ${sql.identifier(
          migrationsSchema
        )}.${sql.identifier(legacySequence)}`
      );
    }
    await session.execute(migrationTableCreate);

    const dbMigrations = await session.all<{
      id: number;
      hash: string;
      created_at: string;
    }>(
      sql`select id, hash, created_at from ${sql.identifier(
        migrationsSchema
      )}.${sql.identifier(migrationsTable)} order by created_at desc limit 1`
    );

    const lastDbMigration = dbMigrations[0];

    await session.transaction(async (tx) => {
      for await (const migration of migrations) {
        if (
          !lastDbMigration ||
          Number(lastDbMigration.created_at) < migration.folderMillis
        ) {
          for (const stmt of migration.sql) {
            await tx.execute(sql.raw(stmt));
          }

          await tx.execute(
            sql`insert into ${sql.identifier(
              migrationsSchema
            )}.${sql.identifier(
              migrationsTable
            )} ("hash", "created_at") values(${migration.hash}, ${
              migration.folderMillis
            })`
          );
        }
      }
    });
  }

  override prepareTyping(
    encoder: DriverValueEncoder<unknown, unknown>
  ): QueryTypingsValue {
    if (is(encoder, PgJsonb) || is(encoder, PgJson)) {
      this.hasPgJsonColumn = true;
      return 'none';
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
