import type {
  Register,
  BalanceOptions,
  TurnoverOptions,
  MovementsOptions,
  ResourceResult,
  TurnoverRow,
  MovementRow,
} from '../types/register';
import type { DimensionFilter } from '../types/pg-types';
import { toTimestamp, sqlIdentifier } from '../utils';
import { mapPgError } from '../errors';

type Executor = { $queryRawUnsafe: (...args: unknown[]) => Promise<unknown[]> };

/**
 * Query current or historical balance.
 */
export async function balance<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: Executor,
  schema: string,
  register: Register<D, R>,
  dims?: DimensionFilter<D>,
  options?: BalanceOptions,
): Promise<ResourceResult<R> | null> {
  const name = register._def.name;
  const fnName = `${name}_balance`;

  const params: unknown[] = [];
  const parts: string[] = [];
  let idx = 1;

  if (dims && Object.keys(dims).length > 0) {
    parts.push(`dimensions := $${idx}::jsonb`);
    params.push(JSON.stringify(dims));
    idx++;
  }

  if (options?.atDate) {
    parts.push(`at_date := $${idx}::timestamptz`);
    params.push(toTimestamp(options.atDate));
    idx++;
  }

  const argList = parts.length > 0 ? parts.join(', ') : '';
  const sql = `SELECT * FROM "${schema}".${sqlIdentifier(fnName)}(${argList})`;

  try {
    const rows = await executor.$queryRawUnsafe(sql, ...params);
    if (!rows || rows.length === 0) return null;
    const row = rows[0] as Record<string, unknown>;
    // The function returns a single JSONB column or flattened resources
    const result = row[fnName] ?? row;
    if (typeof result === 'string') {
      return JSON.parse(result) as ResourceResult<R>;
    }
    return result as ResourceResult<R>;
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Query turnover for a period.
 */
export async function turnover<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: Executor,
  schema: string,
  register: Register<D, R>,
  dims?: DimensionFilter<D>,
  options?: TurnoverOptions,
): Promise<TurnoverRow<R>[]> {
  const name = register._def.name;
  const fnName = `${name}_turnover`;

  const params: unknown[] = [];
  const parts: string[] = [];
  let idx = 1;

  if (options?.dateFrom) {
    parts.push(`from_date := $${idx}::timestamptz`);
    params.push(toTimestamp(options.dateFrom));
    idx++;
  }

  if (options?.dateTo) {
    parts.push(`to_date := $${idx}::timestamptz`);
    params.push(toTimestamp(options.dateTo));
    idx++;
  }

  if (dims && Object.keys(dims).length > 0) {
    parts.push(`dimensions := $${idx}::jsonb`);
    params.push(JSON.stringify(dims));
    idx++;
  }

  if (options?.groupBy && options.groupBy.length > 0) {
    parts.push(`group_by := $${idx}::jsonb`);
    params.push(JSON.stringify(options.groupBy));
    idx++;
  }

  const argList = parts.length > 0 ? parts.join(', ') : '';
  const sql = `SELECT * FROM "${schema}".${sqlIdentifier(fnName)}(${argList})`;

  try {
    const rows = await executor.$queryRawUnsafe(sql, ...params);
    return (rows as Record<string, unknown>[]).map((row) => {
      const val = row[fnName] ?? row;
      if (typeof val === 'string') return JSON.parse(val);
      return val;
    }) as TurnoverRow<R>[];
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Query movements with filters and pagination.
 */
export async function movements<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: Executor,
  schema: string,
  register: Register<D, R>,
  dims?: DimensionFilter<D>,
  options?: MovementsOptions,
): Promise<MovementRow<D, R>[]> {
  const name = register._def.name;
  const fnName = `${name}_movements`;

  const params: unknown[] = [];
  const parts: string[] = [];
  let idx = 1;

  if (options?.recorder) {
    parts.push(`p_recorder := $${idx}`);
    params.push(options.recorder);
    idx++;
  }

  if (options?.dateFrom) {
    parts.push(`from_date := $${idx}::timestamptz`);
    params.push(toTimestamp(options.dateFrom));
    idx++;
  }

  if (options?.dateTo) {
    parts.push(`to_date := $${idx}::timestamptz`);
    params.push(toTimestamp(options.dateTo));
    idx++;
  }

  if (dims && Object.keys(dims).length > 0) {
    parts.push(`dimensions := $${idx}::jsonb`);
    params.push(JSON.stringify(dims));
    idx++;
  }

  const argList = parts.length > 0 ? parts.join(', ') : '';
  let sql = `SELECT * FROM "${schema}".${sqlIdentifier(fnName)}(${argList})`;

  if (options?.limit) {
    sql += ` LIMIT ${Number(options.limit)}`;
  }
  if (options?.offset) {
    sql += ` OFFSET ${Number(options.offset)}`;
  }

  try {
    const rows = await executor.$queryRawUnsafe(sql, ...params);
    return (rows as Record<string, unknown>[]).map((row) => {
      const val = row[fnName] ?? row;
      if (typeof val === 'string') return JSON.parse(val);
      return val;
    }) as MovementRow<D, R>[];
  } catch (err) {
    mapPgError(err);
  }
}
