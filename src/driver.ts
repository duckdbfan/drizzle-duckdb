import { DuckDBInstance } from '@duckdb/node-api';
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
import { closeClientConnection, isPool } from './client.ts';
import {
  createDuckDBConnectionPool,
  resolvePoolSize,
  type DuckDBPoolConfig,
  type PoolPreset,
} from './pool.ts';

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

/** Connection configuration when using path-based connection */
export interface DuckDBConnectionConfig {
  /** Database path: ':memory:', './file.duckdb', 'md:', 'md:database' */
  path: string;
  /** DuckDB instance options (e.g., motherduck_token) */
  options?: Record<string, string>;
}

export interface DuckDBDrizzleConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DrizzleConfig<TSchema> {
  rewriteArrays?: boolean;
  rejectStringArrayLiterals?: boolean;
  /** Pool configuration. Use preset name, size config, or false to disable. */
  pool?: DuckDBPoolConfig | PoolPreset | false;
}

export interface DuckDBDrizzleConfigWithConnection<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DuckDBDrizzleConfig<TSchema> {
  /** Connection string or config object */
  connection: string | DuckDBConnectionConfig;
}

export interface DuckDBDrizzleConfigWithClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DuckDBDrizzleConfig<TSchema> {
  /** Explicit client (connection or pool) */
  client: DuckDBClientLike;
}

/** Check if a value looks like a config object (not a client) */
function isConfigObject(data: unknown): data is Record<string, unknown> {
  if (typeof data !== 'object' || data === null) return false;
  if (data.constructor?.name !== 'Object') return false;
  return (
    'connection' in data ||
    'client' in data ||
    'pool' in data ||
    'schema' in data ||
    'logger' in data
  );
}

/** Internal: create database from a client (connection or pool) */
function createFromClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: DuckDBClientLike,
  config: DuckDBDrizzleConfig<TSchema> = {},
  instance?: DuckDBInstance
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

  const db = new DuckDBDatabase(dialect, session, schema, client, instance);
  return db as DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;
}

/** Internal: create database from a connection string */
async function createFromConnectionString<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  path: string,
  instanceOptions: Record<string, string> | undefined,
  config: DuckDBDrizzleConfig<TSchema> = {}
): Promise<DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  const instance = await DuckDBInstance.create(path, instanceOptions);
  const poolSize = resolvePoolSize(config.pool);

  if (poolSize === false) {
    const connection = await instance.connect();
    return createFromClient(connection, config, instance);
  }

  const pool = createDuckDBConnectionPool(instance, { size: poolSize });
  return createFromClient(pool, config, instance);
}

// Overload 1: Connection string (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  connectionString: string
): Promise<DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 2: Connection string + config (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  connectionString: string,
  config: DuckDBDrizzleConfig<TSchema>
): Promise<DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 3: Config with connection (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: DuckDBDrizzleConfigWithConnection<TSchema>
): Promise<DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 4: Config with explicit client (sync)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: DuckDBDrizzleConfigWithClient<TSchema>
): DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Overload 5: Explicit client (sync, backward compatible)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: DuckDBClientLike,
  config?: DuckDBDrizzleConfig<TSchema>
): DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Implementation
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  clientOrConfigOrPath:
    | string
    | DuckDBClientLike
    | DuckDBDrizzleConfigWithConnection<TSchema>
    | DuckDBDrizzleConfigWithClient<TSchema>,
  config?: DuckDBDrizzleConfig<TSchema>
):
  | DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>
  | Promise<DuckDBDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  // String path -> async with auto-pool
  if (typeof clientOrConfigOrPath === 'string') {
    return createFromConnectionString(clientOrConfigOrPath, undefined, config);
  }

  // Config object with connection or client
  if (isConfigObject(clientOrConfigOrPath)) {
    const configObj = clientOrConfigOrPath as
      | DuckDBDrizzleConfigWithConnection<TSchema>
      | DuckDBDrizzleConfigWithClient<TSchema>;

    if ('connection' in configObj) {
      const connConfig =
        configObj as DuckDBDrizzleConfigWithConnection<TSchema>;
      const { connection, ...restConfig } = connConfig;
      if (typeof connection === 'string') {
        return createFromConnectionString(
          connection,
          undefined,
          restConfig as DuckDBDrizzleConfig<TSchema>
        );
      }
      return createFromConnectionString(
        connection.path,
        connection.options,
        restConfig as DuckDBDrizzleConfig<TSchema>
      );
    }

    if ('client' in configObj) {
      const clientConfig = configObj as DuckDBDrizzleConfigWithClient<TSchema>;
      const { client: clientValue, ...restConfig } = clientConfig;
      return createFromClient(
        clientValue,
        restConfig as DuckDBDrizzleConfig<TSchema>
      );
    }

    throw new Error(
      'Invalid drizzle config: either connection or client must be provided'
    );
  }

  // Direct client (backward compatible)
  return createFromClient(clientOrConfigOrPath as DuckDBClientLike, config);
}

export class DuckDBDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends
    TablesRelationalConfig = ExtractTablesWithRelations<TFullSchema>,
> extends PgDatabase<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBDatabase';

  /** The underlying connection or pool */
  readonly $client: DuckDBClientLike;

  /** The DuckDB instance (when created from connection string) */
  readonly $instance?: DuckDBInstance;

  constructor(
    readonly dialect: DuckDBDialect,
    readonly session: DuckDBSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    client: DuckDBClientLike,
    instance?: DuckDBInstance
  ) {
    super(dialect, session, schema);
    this.$client = client;
    this.$instance = instance;
  }

  /**
   * Close the database connection pool and instance.
   * Should be called when shutting down the application.
   */
  async close(): Promise<void> {
    if (isPool(this.$client) && this.$client.close) {
      await this.$client.close();
    }
    if (!isPool(this.$client)) {
      await closeClientConnection(this.$client);
    }
    if (this.$instance) {
      const maybeClosable = this.$instance as unknown as {
        close?: () => Promise<void> | void;
        closeSync?: () => void;
      };
      if (typeof maybeClosable.close === 'function') {
        await maybeClosable.close();
      } else if (typeof maybeClosable.closeSync === 'function') {
        maybeClosable.closeSync();
      }
    }
  }

  select(): DuckDBSelectBuilder<undefined>;
  select<TSelection extends SelectedFields>(
    fields: TSelection
  ): DuckDBSelectBuilder<TSelection>;
  select(
    fields?: SelectedFields
  ): DuckDBSelectBuilder<SelectedFields | undefined> {
    const selectedFields = fields ? aliasFields(fields) : undefined;

    // Cast needed: DuckDBSession is compatible but types don't align exactly with PgSession
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
