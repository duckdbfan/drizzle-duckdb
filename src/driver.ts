import { entityKind, is } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { DefaultLogger } from 'drizzle-orm/logger';
import { PgDatabase } from 'drizzle-orm/pg-core/db';
import { PgDialect } from 'drizzle-orm/pg-core/dialect';
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type ExtractTablesWithRelations,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { getTableColumns, type DrizzleConfig, type DrizzleTypeError } from 'drizzle-orm/utils';
import type {
  DuckDBClient,
  DuckDBQueryResultHKT,
  DuckDBTransaction,
} from './session';
import { DuckDBSession } from './session.ts';
import { DuckDBDialect } from './dialect.ts';

import {
  PgSelectBase,
  PgSelectBuilder,
  type CreatePgSelectFromBuilderMode,
  type SelectedFields,
  type TableLikeHasEmptySelection,
} from 'drizzle-orm/pg-core/query-builders';
import {
  PgColumn,
  PgTable,
  type PgSession,
  type PgTransactionConfig,
} from 'drizzle-orm/pg-core';
import { sql, SQL, type ColumnsSelection } from 'drizzle-orm/sql/sql';
import { Column } from 'drizzle-orm/column';
import { Subquery, ViewBaseConfig, type SQLWrapper } from 'drizzle-orm';
import { PgViewBase } from 'drizzle-orm/pg-core/view-base';
import type {
  GetSelectTableName,
  GetSelectTableSelection,
} from 'drizzle-orm/query-builders/select.types';
import { aliasFields } from './utils.ts';

export interface PgDriverOptions {
  logger?: Logger;
}

const selectionRegex = /select\s+(.+)\s+from/i;
const aliasRegex = /as\s+(.+)$/i;

export class DuckDBDriver {
  static readonly [entityKind]: string = 'DuckDBDriver';

  constructor(
    private client: DuckDBClient,
    private dialect: DuckDBDialect,
    private options: PgDriverOptions = {}
  ) {
    // const clientProxy = new Proxy(client, {
    //   get(target, prop, receiver) {
    //     if (prop === 'all') {
    //       return (query: string, params: Parameters<typeof target.all>) => {
    //         // DuckDB names returned variables differently than Postgres
    //         // so we need to remap them to match the Postgres names
    //         const selection = selectionRegex.exec(query);
    //         if (selection?.length !== 2) {
    //           return target.all(query, params);
    //         }
    //         const fields = selection[1].split(',').map((field) => {});
    //         return target.all(query, params);
    //       };
    //     }
    //     return Reflect.get(target, prop, receiver);
    //   },
    // });
  }

  createSession(
    schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined
  ): DuckDBSession<Record<string, unknown>, TablesRelationalConfig> {
    return new DuckDBSession(this.client, this.dialect, schema, {
      logger: this.options.logger,
    });
  }
}

// Need to work around omitted internal types from drizzle...
// interface DuckDBDatabaseInternal<
//   TSchema extends Record<string, unknown> = Record<string, never>
// > extends PgDatabase<DuckDBQueryResultHKT, TSchema> {
//   dialect: PgDialect;
//   session: DuckDBSession<Record<string, unknown>, TablesRelationalConfig>;
// }

// export type DuckDBDatabase<
//   TSchema extends Record<string, unknown> = Record<string, never>
// > = DuckDBDatabaseInternal<TSchema>;

export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>
>(
  client: DuckDBClient,
  config: DrizzleConfig<TSchema> = {}
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

  const driver = new DuckDBDriver(client, dialect, { logger });
  const session = driver.createSession(schema);

  return new DuckDBDatabase(dialect, session, schema) as DuckDBDatabase<
    TSchema,
    ExtractTablesWithRelations<TSchema>
  >;
}

export class DuckDBDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = ExtractTablesWithRelations<TFullSchema>
> extends PgDatabase<DuckDBQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DuckDBDatabase';

  constructor(
    readonly dialect: DuckDBDialect,
    readonly session: DuckDBSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined
  ) {
    super(dialect, session, schema);
  }

  select(): PgSelectBuilder<undefined>;
  select<TSelection extends SelectedFields>(
    fields: TSelection
  ): PgSelectBuilder<TSelection>;
  select(fields?: SelectedFields): PgSelectBuilder<SelectedFields | undefined> {
    if (!fields) {
      // iterate over all fields and do the same as below (may have to extend/override `.from()`)

      return new DuckDBSelectBuilder({
        fields: fields ?? undefined,
        session: this.session as unknown as PgSession<DuckDBQueryResultHKT>,
        dialect: this.dialect,
      });
    }

    const aliasedFields: SelectedFields = aliasFields(fields);

    return new DuckDBSelectBuilder({
      fields: aliasedFields,
      session: this.session as unknown as PgSession<DuckDBQueryResultHKT>,
      dialect: this.dialect,
    });
  }

  override async transaction<T>(
    transaction: (tx: DuckDBTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    return await this.session.transaction<T>(transaction);
  }
}

interface PgViewBaseInternal<
  TName extends string = string,
  TExisting extends boolean = boolean,
  TSelectedFields extends ColumnsSelection = ColumnsSelection
> extends PgViewBase<TName, TExisting, TSelectedFields> {
  [ViewBaseConfig]?: {
    selectedFields: SelectedFields;
  };
}

export class DuckDBSelectBuilder<
  TSelection extends SelectedFields | undefined,
  TBuilderMode extends 'db' | 'qb' = 'db'
> extends PgSelectBuilder<TSelection, TBuilderMode> {
  private _fields: TSelection;
  private _session: PgSession | undefined;
  private _dialect: DuckDBDialect;
  private _withList: Subquery[] = [];
  private _distinct:
    | boolean
    | {
        on: (PgColumn | SQLWrapper)[];
      }
    | undefined;

  constructor(config: {
    fields: TSelection;
    session: PgSession | undefined;
    dialect: PgDialect;
    withList?: Subquery[];
    distinct?:
      | boolean
      | {
          on: (PgColumn | SQLWrapper)[];
        };
  }) {
    super(config);
    this._fields = config.fields;
    this._session = config.session;
    this._dialect = config.dialect;
    if (config.withList) {
      this._withList = config.withList;
    }
    this._distinct = config.distinct;
  }

  from<TFrom extends PgTable | Subquery | PgViewBaseInternal | SQL>(
    source: TableLikeHasEmptySelection<TFrom> extends true ? DrizzleTypeError<
    "Cannot reference a data-modifying statement subquery if it doesn't contain a `returning` clause"
  >
  : TFrom
  ): CreatePgSelectFromBuilderMode<
    TBuilderMode,
    GetSelectTableName<TFrom>,
    TSelection extends undefined ? GetSelectTableSelection<TFrom> : TSelection,
    TSelection extends undefined ? 'single' : 'partial'
  > {
    const isPartialSelect = !!this._fields;
    const src = source as TFrom;

    let fields: SelectedFields;
    if (this._fields) {
      fields = this._fields;
    } else if (is(src, Subquery)) {
      // This is required to use the proxy handler to get the correct field values from the subquery
      fields = Object.fromEntries(
        Object.keys(src._.selectedFields).map((key) => [
          key,
          src[
            key as unknown as keyof typeof src
          ] as unknown as SelectedFields[string],
        ])
      );
    } else if (is(src, PgViewBase)) {
      fields = src[ViewBaseConfig]?.selectedFields as SelectedFields;
    } else if (is(src, SQL)) {
      fields = {};
    } else {
      fields = aliasFields(getTableColumns<PgTable>(src), !isPartialSelect);
    }

    return new PgSelectBase({
      table: src,
      fields,
      isPartialSelect,
      session: this._session,
      dialect: this._dialect,
      withList: this._withList,
      distinct: this._distinct,
    }) as any;
  }
}
