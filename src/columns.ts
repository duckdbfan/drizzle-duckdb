import { sql, type SQL } from 'drizzle-orm';
import { customType } from 'drizzle-orm/pg-core';

type IntColType =
  | 'SMALLINT'
  | 'INTEGER'
  | 'BIGINT'
  | 'HUGEINT'
  | 'USMALLINT'
  | 'UINTEGER'
  | 'UBIGINT'
  | 'UHUGEINT'
  | 'INT'
  | 'INT16'
  | 'INT32'
  | 'INT64'
  | 'INT128'
  | 'LONG'
  | 'VARINT';

type FloatColType = 'FLOAT' | 'DOUBLE';

type StringColType = 'STRING' | 'VARCHAR' | 'TEXT';

type BoolColType = 'BOOLEAN' | 'BOOL';

type BlobColType = 'BLOB' | 'BYTEA' | 'VARBINARY';

type DateColType =
  | 'DATE'
  | 'TIME'
  | 'TIMETZ'
  | 'TIMESTAMP'
  | 'DATETIME'
  | 'TIMESTAMPTZ'
  | 'TIMESTAMP_MS'
  | 'TIMESTAMP_S';

type AnyColType =
  | IntColType
  | FloatColType
  | StringColType
  | BoolColType
  | DateColType
  | BlobColType;

type ListColType = `${AnyColType}[]`;
type ArrayColType = `${AnyColType}[${number}]`;
/**
 * @example
 * const structColType: StructColType = 'STRUCT (name: STRING, age: INT)';
 */
type StructColType = `STRUCT (${string})`;

export const duckDbMap = <TData extends Record<string, any>>(
  name: string,
  valueType: AnyColType | ListColType | ArrayColType
) =>
  customType<{ data: TData; driverData: string }>({
    dataType() {
      console.log('dataType');
      return `MAP (STRING, ${valueType})`;
    },
    toDriver(value: TData) {
      console.log('toDriver');
      // todo: more sophisticated encoding based on data type
      const valueFormatter = (value: any) => {
        if (['STRING', 'TEXT', 'VARCHAR'].includes(valueType)) {
          return `'${value}'`;
        }

        return JSON.stringify(value);
      };

      const values = Object.entries(value).map(([key, value]) => {
        return sql.raw(`'${key}': ${valueFormatter(value)}`);
      });

      const sqlChunks: SQL[] = [];

      for (const value of values) {
        sqlChunks.push(value);
      }

      return sql`MAP {${sql.join(sqlChunks, sql.raw(', '))}}`;
    },
    // ! this won't actually ever work because of how map values are returned
    fromDriver(value: string): TData {
      console.log('fromDriver');
      // todo: more sophisticated decoding based on data type

      const replacedValue = value.replaceAll(
        /(?:^{)?([^=]+?)=(.+)(?:}$)?/g,
        '"$1":"$2"'
      );
      const formattedValue = `{${replacedValue}}`;

      const valueObj = JSON.parse(formattedValue);

      return Object.fromEntries(
        Object.entries(valueObj).map(([key, value]) => {
          return [key, JSON.parse(value as string)];
        })
      ) as TData;
    },
  })(name);

export const duckDbStruct = <TData extends Record<string, any>>(
  name: string,
  schema: Record<string, AnyColType | ListColType | ArrayColType>
) =>
  customType<{ data: TData; driverData: string }>({
    dataType() {
      const fields = Object.entries(schema).map(
        ([key, type]) => `${key} ${type}`
      );

      return `STRUCT (${fields.join(', ')})`;
    },
    toDriver(value: TData) {
      // todo: more sophisticated encoding based on data type
      const valueFormatter = (value: any) =>
        JSON.stringify(value).replaceAll(/(?<!\\)"/g, "'");

      const values = Object.entries(value).map(([key, value]) => {
        return sql.raw(`'${key}': ${valueFormatter(value)}`);
      });

      const sqlChunks: SQL[] = [];

      for (const value of values) {
        sqlChunks.push(value);
      }

      return sql`(SELECT {${sql.join(sqlChunks, sql.raw(', '))}})`;
    },
    fromDriver(value: string): TData {
      return value as unknown as TData;
    },
  })(name);

export const duckDbBlob = customType<{
  data: Buffer;
  default: false;
}>({
  dataType() {
    return 'BLOB';
  },
  toDriver(value: Buffer) {
    const hexString = value.toString('hex');
    return sql`from_hex(${hexString})`;
  },
});

// todo: date/time types