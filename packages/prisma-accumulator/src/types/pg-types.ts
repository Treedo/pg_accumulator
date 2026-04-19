/**
 * PostgreSQL → TypeScript type mapping for pg_accumulator.
 */

/** Supported PostgreSQL column types for dimensions and resources */
export type PgColumnType =
  | 'int'
  | 'integer'
  | 'bigint'
  | 'numeric'
  | 'decimal'
  | 'text'
  | 'varchar'
  | 'boolean'
  | 'date'
  | 'timestamptz'
  | 'timestamp'
  | 'uuid';

/** Map a PostgreSQL type string to a TypeScript type */
export type PgToTs<T extends string> =
  T extends 'int' | 'integer' ? number :
  T extends 'bigint' ? bigint | number :
  T extends 'numeric' | 'decimal' ? number | string :
  T extends 'text' | 'varchar' | 'uuid' ? string :
  T extends 'boolean' ? boolean :
  T extends 'date' | 'timestamptz' | 'timestamp' ? string | Date :
  unknown;

/** Map a record of {name: pgType} to {name: tsType} */
export type MapPgFields<T extends Record<string, string>> = {
  [K in keyof T]: PgToTs<T[K]>;
};

/** Partial dimensions filter — all fields optional */
export type DimensionFilter<T extends Record<string, string>> = Partial<MapPgFields<T>>;

/** Resource result — numeric fields */
export type ResourceResult<T extends Record<string, string>> = {
  [K in keyof T]: number;
};
