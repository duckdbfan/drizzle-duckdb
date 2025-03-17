import type { MigrationConfig } from 'drizzle-orm/migrator';
import { readMigrationFiles } from 'drizzle-orm/migrator';
import type { DuckDBDatabase } from './driver';
import type { PgSession } from 'drizzle-orm/pg-core/session';

export async function migrate<TSchema extends Record<string, unknown>>(
  db: DuckDBDatabase<TSchema>,
  config: string | MigrationConfig
) {
  const migrations = readMigrationFiles(config);

  await db.dialect.migrate(
    migrations,
    // Need to work around omitted internal types from drizzle...
    db.session as unknown as PgSession,
    config
  );
}
