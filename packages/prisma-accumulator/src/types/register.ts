import type { MapPgFields, DimensionFilter, ResourceResult } from './pg-types';
export type { ResourceResult } from './pg-types';

/** Register kind */
export type RegisterKind = 'balance' | 'turnover';

/** Totals aggregation period */
export type TotalsPeriod = 'day' | 'month' | 'year';

/** Partitioning strategy */
export type PartitionBy = 'day' | 'month' | 'quarter' | 'year';

/** PostgreSQL type for recorder column */
export type RecorderType = 'text' | 'int' | 'bigint' | 'uuid';

/** Register definition input */
export interface RegisterDefinition<
  D extends Record<string, string> = Record<string, string>,
  R extends Record<string, string> = Record<string, string>,
> {
  name: string;
  kind: RegisterKind;
  dimensions: D;
  resources: R;
  totals_period?: TotalsPeriod;
  partition_by?: PartitionBy;
  high_write?: boolean;
  recorder_type?: RecorderType;
}

/** A typed register handle returned by defineRegister() */
export interface Register<
  D extends Record<string, string> = Record<string, string>,
  R extends Record<string, string> = Record<string, string>,
> {
  readonly _def: RegisterDefinition<D, R>;
}

/** Movement data for posting */
export type MovementInput<
  D extends Record<string, string>,
  R extends Record<string, string>,
> = {
  recorder: string;
  period: string | Date;
} & MapPgFields<D> & Partial<MapPgFields<R>>;

/** Balance query options */
export interface BalanceOptions {
  atDate?: string | Date;
}

/** Turnover query options */
export interface TurnoverOptions {
  dateFrom?: string | Date;
  dateTo?: string | Date;
  groupBy?: string[];
}

/** Movements query options */
export interface MovementsOptions {
  recorder?: string;
  dateFrom?: string | Date;
  dateTo?: string | Date;
  limit?: number;
  offset?: number;
}

/** Turnover result row (resources + optional grouped dimensions) */
export type TurnoverRow<R extends Record<string, string>> = ResourceResult<R> & Record<string, unknown>;

/** Movement result row */
export type MovementRow<
  D extends Record<string, string>,
  R extends Record<string, string>,
> = {
  id: number;
  recorded_at: string;
  recorder: string;
  period: string;
  movement_type: string;
  dim_hash: string;
} & MapPgFields<D> & ResourceResult<R>;

/** Register info returned by register_info() */
export interface RegisterInfo {
  name: string;
  kind: RegisterKind;
  dimensions: Record<string, string>;
  resources: Record<string, string>;
  totals_period: TotalsPeriod;
  partition_by: PartitionBy;
  high_write: boolean;
  recorder_type: string;
  created_at: string;
  movements_count: number;
  tables: Record<string, string>;
  partitions?: unknown[];
}

/** Register list row */
export interface RegisterListRow {
  name: string;
  kind: RegisterKind;
  dimensions: number;
  resources: number;
  movements_count: number;
  created_at: string;
}

/** Alter register options */
export interface AlterRegisterOptions {
  addDimensions?: Record<string, string>;
  addResources?: Record<string, string>;
  highWrite?: boolean;
}

/** Client configuration */
export interface AccumulatorConfig {
  schema?: string;
}
