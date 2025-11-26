import type { MigrationConfig } from 'drizzle-orm/migrator';
import { readMigrationFiles } from 'drizzle-orm/migrator';
import type { DuckDBDatabase } from './driver.ts';
import type { PgSession } from 'drizzle-orm/pg-core/session';

export type DuckDbMigrationConfig = MigrationConfig | string;

export async function migrate<TSchema extends Record<string, unknown>>(
  db: DuckDBDatabase<TSchema>,
  config: DuckDbMigrationConfig
) {
  const migrationConfig: MigrationConfig =
    typeof config === 'string' ? { migrationsFolder: config } : config;

  const migrations = readMigrationFiles(migrationConfig);

  await db.dialect.migrate(
    migrations,
    // Need to work around omitted internal types from drizzle...
    db.session as unknown as PgSession,
    migrationConfig
  );
}
