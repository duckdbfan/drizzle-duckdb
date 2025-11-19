import type { Connection, Database, RowData } from 'duckdb-async';
import type {
  DuckDBConnection as NodeApiDuckDBConnection,
  DuckDBValue as NodeApiDuckDBValue,
} from '@duckdb/node-api';
import { entityKind } from 'drizzle-orm/entity';
import { type Logger, NoopLogger } from 'drizzle-orm/logger';
import type { PgDialect } from 'drizzle-orm/pg-core/dialect';
import { PgTransaction } from 'drizzle-orm/pg-core';
import type { SelectedFieldsOrdered } from 'drizzle-orm/pg-core/query-builders/select.types';
import type {
  PgTransactionConfig,
  PreparedQueryConfig,
  PgQueryResultHKT,
} from 'drizzle-orm/pg-core/session';
import { PgPreparedQuery, PgSession } from 'drizzle-orm/pg-core/session';
import type {
  RelationalSchemaConfig,
  TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { fillPlaceholders, type Query, SQL, sql } from 'drizzle-orm/sql/sql';
import type { Assume } from 'drizzle-orm/utils';
import { mapResultRow } from './utils';
import type { DuckDBDialect } from './dialect';
import { TransactionRollbackError } from 'drizzle-orm/errors';

export type DuckDBClient = Database;

export type DuckDBClientLike =
  | DuckDBClient
  | Connection
  | NodeApiDuckDBConnection;

export class DuckDBPreparedQuery<
  T extends PreparedQueryConfig
> extends PgPreparedQuery<T> {
  static readonly [entityKind]: string = 'DuckDBPreparedQuery';

  // private rawQueryConfig: QueryOptions;
  // private queryConfig: QueryOptions;

  constructor(
    private client: DuckDBClientLike,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper?: (rows: unknown[][]) => T['execute']
  ) {
    super({ sql: queryString, params });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['execute']> {
    const params = fillPlaceholders(this.params, placeholderValues);

    this.logger.logQuery(this.queryString, params);

    const {
      fields,
      joinsNotNullableMap,
      customResultMapper,
      queryString,
    } = this as typeof this & { joinsNotNullableMap?: Record<string, boolean> };

    const rows = await executeOnClient(this.client, queryString, params);

    if (rows.length === 0 || !fields) {
      return rows;
    }

    const rowValues = rows.map((row) => Object.values(row));

    return customResultMapper
      ? customResultMapper(rowValues)
      : rowValues.map((row) =>
          mapResultRow<T['execute']>(fields!, row, joinsNotNullableMap)
        );
  }

  all(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['all']> {
    return this.execute(placeholderValues);
  }

  isResponseInArrayMode(): boolean {
    return false;
  }
}

export interface DuckDBSessionOptions {
  logger?: Logger;
}

export class DuckDBSession<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>
> extends PgSession<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBSession';

  private logger: Logger;

  constructor(
    private client: DuckDBClientLike,
    dialect: DuckDBDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: DuckDBSessionOptions = {}
  ) {
    super(dialect);
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
    query: Query,
    fields: SelectedFieldsOrdered | undefined,
    name: string | undefined,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][]) => T['execute']
  ): PgPreparedQuery<T> {
    return new DuckDBPreparedQuery(
      this.client,
      query.sql,
      query.params,
      this.logger,
      fields,
      isResponseInArrayMode,
      customResultMapper
    );
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    const client = this.client;
    const clientHasConnect = hasConnect(client);
    const connection = clientHasConnect
      ? await client.connect()
      : client;

    const session = new DuckDBSession(
      connection,
      this.dialect,
      this.schema,
      this.options
    );

    const tx = new DuckDBTransaction<TFullSchema, TSchema>(
      this.dialect,
      session,
      this.schema
    );

    await tx.execute(sql`BEGIN TRANSACTION;`);

    try {
      const result = await transaction(tx);
      await tx.execute(sql`commit`);
      return result;
    } catch (error) {
      await tx.execute(sql`rollback`);
      throw error;
    } finally {
      if (clientHasConnect) {
        await closeClientConnection(connection);
      }
    }
  }
}

type PgTransactionInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>
> = {
  dialect: DuckDBDialect;
  session: DuckDBSession<TFullSchema, TSchema>;
};

type DuckDBTransactionWithInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>
> = PgTransactionInternals<TFullSchema, TSchema> &
  DuckDBTransaction<TFullSchema, TSchema>;

export class DuckDBTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig
> extends PgTransaction<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBTransaction';

  rollback(): never {
    throw new TransactionRollbackError();
  }

  getTransactionConfigSQL(config: PgTransactionConfig): SQL {
    const chunks: string[] = [];
    if (config.isolationLevel) {
      chunks.push(`isolation level ${config.isolationLevel}`);
    }
    if (config.accessMode) {
      chunks.push(config.accessMode);
    }
    if (typeof config.deferrable === 'boolean') {
      chunks.push(config.deferrable ? 'deferrable' : 'not deferrable');
    }
    return sql.raw(chunks.join(' '));
  }

  setTransaction(config: PgTransactionConfig): Promise<void> {
    // Need to work around omitted internal types from drizzle...
    type Tx = DuckDBTransactionWithInternals<TFullSchema, TSchema>;
    return (this as unknown as Tx).session.execute(
      sql`set transaction ${this.getTransactionConfigSQL(config)}`
    );
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    // Need to work around omitted internal types from drizzle...
    type Tx = DuckDBTransactionWithInternals<TFullSchema, TSchema>;

    const savepointName = `sp${this.nestedIndex + 1}`;
    const tx = new DuckDBTransaction<TFullSchema, TSchema>(
      (this as unknown as Tx).dialect,
      (this as unknown as Tx).session,
      this.schema,
      this.nestedIndex + 1
    );
    await tx.execute(sql.raw(`savepoint ${savepointName}`));
    try {
      const result = await transaction(tx);
      await tx.execute(sql.raw(`release savepoint ${savepointName}`));
      return result;
    } catch (err) {
      await tx.execute(sql.raw(`rollback to savepoint ${savepointName}`));
      throw err;
    }
  }
}

function isDuckDbAsyncClient(
  client: DuckDBClientLike
): client is DuckDBClient | Connection {
  return typeof (client as Connection).all === 'function';
}

function isNodeApiClient(
  client: DuckDBClientLike
): client is NodeApiDuckDBConnection {
  return typeof (client as NodeApiDuckDBConnection).run === 'function';
}

function hasConnect(client: DuckDBClientLike): client is DuckDBClient {
  return typeof (client as DuckDBClient).connect === 'function';
}

async function closeClientConnection(
  connection: Connection | NodeApiDuckDBConnection
): Promise<void> {
  if ('close' in connection && typeof connection.close === 'function') {
    await connection.close();
    return;
  }

  if (
    'closeSync' in connection &&
    typeof connection.closeSync === 'function'
  ) {
    connection.closeSync();
    return;
  }

  if (
    'disconnectSync' in connection &&
    typeof connection.disconnectSync === 'function'
  ) {
    connection.disconnectSync();
  }
}

async function executeOnClient(
  client: DuckDBClientLike,
  query: string,
  params: unknown[]
): Promise<RowData[]> {
  if (isDuckDbAsyncClient(client)) {
    return (await client.all(query, ...params)) ?? [];
  }

  if (isNodeApiClient(client)) {
    const values =
      params.length > 0 ? (params as NodeApiDuckDBValue[]) : undefined;
    const result = await client.run(query, values);
    const rows = await result.getRowObjectsJS();
    return (rows ?? []) as RowData[];
  }

  throw new Error(
    'Unsupported DuckDB client: expected duckdb-async Database/Connection or @duckdb/node-api Connection.'
  );
}

export type GenericRowData<T extends RowData = RowData> = T;

export type GenericTableData<T = RowData> = T[];

export interface DuckDBQueryResultHKT extends PgQueryResultHKT {
  type: GenericTableData<Assume<this['row'], RowData>>;
}
