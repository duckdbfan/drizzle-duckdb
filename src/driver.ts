import { entityKind } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { DefaultLogger } from 'drizzle-orm/logger';
import { PgDatabase } from 'drizzle-orm/pg-core/db';
import type { SelectedFields } from 'drizzle-orm/pg-core/query-builders';
import type { PgSession } from 'drizzle-orm/pg-core';
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type ExtractTablesWithRelations,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { type DrizzleConfig } from 'drizzle-orm/utils';
import type { SQL } from 'drizzle-orm/sql/sql';
import type {
  DuckDBClientLike,
  DuckDBQueryResultHKT,
  DuckDBTransaction,
} from './session.ts';
import { DuckDBSession } from './session.ts';
import { DuckDBDialect } from './dialect.ts';
import { DuckDBSelectBuilder } from './select-builder.ts';
import { aliasFields } from './sql/selection.ts';
import type { ExecuteInBatchesOptions, RowData } from './client.ts';

export interface PgDriverOptions {
  logger?: Logger;
  rewriteArrays?: boolean;
  rejectStringArrayLiterals?: boolean;
}

export class DuckDBDriver {
  static readonly [entityKind]: string = 'DuckDBDriver';

  constructor(
    private client: DuckDBClientLike,
    private dialect: DuckDBDialect,
    private options: PgDriverOptions = {}
  ) {}

  createSession(
    schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined
  ): DuckDBSession<Record<string, unknown>, TablesRelationalConfig> {
    return new DuckDBSession(this.client, this.dialect, schema, {
      logger: this.options.logger,
      rewriteArrays: this.options.rewriteArrays,
      rejectStringArrayLiterals: this.options.rejectStringArrayLiterals,
    });
  }
}

export interface DuckDBDrizzleConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DrizzleConfig<TSchema> {
  rewriteArrays?: boolean;
  rejectStringArrayLiterals?: boolean;
}

export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: DuckDBClientLike,
  config: DuckDBDrizzleConfig<TSchema> = {}
): DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>> {
  const dialect = new DuckDBDialect();

  const logger =
    config.logger === true ? new DefaultLogger() : config.logger || undefined;

  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;

  if (config.schema) {
    const tablesConfig = extractTablesRelationalConfig(
      config.schema,
      createTableRelationsHelpers
    );
    schema = {
      fullSchema: config.schema,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  const driver = new DuckDBDriver(client, dialect, {
    logger,
    rewriteArrays: config.rewriteArrays,
    rejectStringArrayLiterals: config.rejectStringArrayLiterals,
  });
  const session = driver.createSession(schema);

  return new DuckDBDatabase(dialect, session, schema) as DuckDBDatabase<
    TSchema,
    ExtractTablesWithRelations<TSchema>
  >;
}

export class DuckDBDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends
    TablesRelationalConfig = ExtractTablesWithRelations<TFullSchema>,
> extends PgDatabase<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBDatabase';

  constructor(
    readonly dialect: DuckDBDialect,
    readonly session: DuckDBSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined
  ) {
    super(dialect, session, schema);
  }

  select(): DuckDBSelectBuilder<undefined>;
  select<TSelection extends SelectedFields>(
    fields: TSelection
  ): DuckDBSelectBuilder<TSelection>;
  select(
    fields?: SelectedFields
  ): DuckDBSelectBuilder<SelectedFields | undefined> {
    const selectedFields = fields ? aliasFields(fields) : undefined;

    return new DuckDBSelectBuilder({
      fields: selectedFields ?? undefined,
      session: this.session as unknown as PgSession<DuckDBQueryResultHKT>,
      dialect: this.dialect,
    });
  }

  executeBatches<T extends RowData = RowData>(
    query: SQL,
    options: ExecuteInBatchesOptions = {}
  ): AsyncGenerator<T[], void, void> {
    return this.session.executeBatches<T>(query, options);
  }

  executeArrow(query: SQL): Promise<unknown> {
    return this.session.executeArrow(query);
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    return await this.session.transaction<T>(transaction);
  }
}
