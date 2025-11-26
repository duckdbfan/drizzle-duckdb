import { entityKind } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { NoopLogger } from 'drizzle-orm/logger';
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
import { adaptArrayOperators } from './sql/query-rewriters.ts';
import { mapResultRow } from './sql/result-mapper.ts';
import type { DuckDBDialect } from './dialect.ts';
import { TransactionRollbackError } from 'drizzle-orm/errors';
import type { DuckDBClientLike, RowData } from './client.ts';
import { executeOnClient, prepareParams } from './client.ts';

export type { DuckDBClientLike, RowData } from './client.ts';

export class DuckDBPreparedQuery<
  T extends PreparedQueryConfig,
> extends PgPreparedQuery<T> {
  static readonly [entityKind]: string = 'DuckDBPreparedQuery';

  constructor(
    private client: DuckDBClientLike,
    private dialect: DuckDBDialect,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper:
      | ((rows: unknown[][]) => T['execute'])
      | undefined,
    private rewriteArrays: boolean,
    private rejectStringArrayLiterals: boolean,
    private warnOnStringArrayLiteral?: (sql: string) => void
  ) {
    super({ sql: queryString, params });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['execute']> {
    this.dialect.assertNoPgJsonColumns();
    const params = prepareParams(
      fillPlaceholders(this.params, placeholderValues),
      {
        rejectStringArrayLiterals: this.rejectStringArrayLiterals,
        warnOnStringArrayLiteral: this.warnOnStringArrayLiteral
          ? () => this.warnOnStringArrayLiteral?.(this.queryString)
          : undefined,
      }
    );
    const rewrittenQuery = this.rewriteArrays
      ? adaptArrayOperators(this.queryString)
      : this.queryString;

    if (this.rewriteArrays && rewrittenQuery !== this.queryString) {
      this.logger.logQuery(
        `[duckdb] original query before array rewrite: ${this.queryString}`,
        params
      );
    }

    this.logger.logQuery(rewrittenQuery, params);

    const { fields, joinsNotNullableMap, customResultMapper } =
      this as typeof this & { joinsNotNullableMap?: Record<string, boolean> };

    const rows = await executeOnClient(this.client, rewrittenQuery, params);

    if (rows.length === 0 || !fields) {
      return rows as T['execute'];
    }

    const rowValues = rows.map((row) => Object.values(row));

    return customResultMapper
      ? customResultMapper(rowValues)
      : rowValues.map((row) =>
          mapResultRow<T['execute']>(fields, row, joinsNotNullableMap)
        );
  }

  all(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['all']> {
    return this.execute(placeholderValues);
  }

  isResponseInArrayMode(): boolean {
    return this._isResponseInArrayMode;
  }
}

export interface DuckDBSessionOptions {
  logger?: Logger;
  rewriteArrays?: boolean;
  rejectStringArrayLiterals?: boolean;
}

export class DuckDBSession<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> extends PgSession<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBSession';

  protected override dialect: DuckDBDialect;
  private logger: Logger;
  private rewriteArrays: boolean;
  private rejectStringArrayLiterals: boolean;
  private hasWarnedArrayLiteral = false;

  constructor(
    private client: DuckDBClientLike,
    dialect: DuckDBDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: DuckDBSessionOptions = {}
  ) {
    super(dialect);
    this.dialect = dialect;
    this.logger = options.logger ?? new NoopLogger();
    this.rewriteArrays = options.rewriteArrays ?? true;
    this.rejectStringArrayLiterals = options.rejectStringArrayLiterals ?? false;
  }

  prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
    query: Query,
    fields: SelectedFieldsOrdered | undefined,
    name: string | undefined,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][]) => T['execute']
  ): PgPreparedQuery<T> {
    void name; // DuckDB doesn't support prepared statement names but the signature must match.
    return new DuckDBPreparedQuery(
      this.client,
      this.dialect,
      query.sql,
      query.params,
      this.logger,
      fields,
      isResponseInArrayMode,
      customResultMapper,
      this.rewriteArrays,
      this.rejectStringArrayLiterals,
      this.rejectStringArrayLiterals ? undefined : this.warnOnStringArrayLiteral
    );
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    const session = new DuckDBSession(
      this.client,
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
    }
  }

  private warnOnStringArrayLiteral = (query: string) => {
    if (this.hasWarnedArrayLiteral) {
      return;
    }
    this.hasWarnedArrayLiteral = true;
    this.logger.logQuery(
      `[duckdb] ${arrayLiteralWarning}\nquery: ${query}`,
      []
    );
  };
}

type PgTransactionInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> = {
  dialect: DuckDBDialect;
  session: DuckDBSession<TFullSchema, TSchema>;
};

type DuckDBTransactionWithInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> = PgTransactionInternals<TFullSchema, TSchema> &
  DuckDBTransaction<TFullSchema, TSchema>;

export class DuckDBTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
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
    type Tx = DuckDBTransactionWithInternals<TFullSchema, TSchema>;
    return (this as unknown as Tx).session.execute(
      sql`set transaction ${this.getTransactionConfigSQL(config)}`
    );
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    type Tx = DuckDBTransactionWithInternals<TFullSchema, TSchema>;
    const nestedTx = new DuckDBTransaction<TFullSchema, TSchema>(
      (this as unknown as Tx).dialect,
      (this as unknown as Tx).session,
      this.schema,
      this.nestedIndex + 1
    );

    return transaction(nestedTx);
  }
}

export type GenericRowData<T extends RowData = RowData> = T;

export type GenericTableData<T = RowData> = T[];

const arrayLiteralWarning =
  'Received a stringified Postgres-style array literal. Use duckDbList()/duckDbArray() or pass native arrays instead. You can also set rejectStringArrayLiterals=true to throw.';

export interface DuckDBQueryResultHKT extends PgQueryResultHKT {
  type: GenericTableData<Assume<this['row'], RowData>>;
}
