import type { Register, MovementInput } from '../types/register';
import { validateMovement } from '../utils';
import { mapPgError } from '../errors';

/**
 * Post movements to a register.
 * Supports single movement or batch (array).
 */
export async function post<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: { $queryRawUnsafe: <T = unknown>(query: string, ...values: unknown[]) => Promise<T> },
  schema: string,
  register: Register<D, R>,
  data: MovementInput<D, R> | MovementInput<D, R>[],
): Promise<number> {
  const movements = Array.isArray(data) ? data : [data];
  for (const m of movements) {
    validateMovement(m as unknown as Record<string, unknown>, register);
  }

  const jsonData = JSON.stringify(Array.isArray(data) ? data : data);

  try {
    const result = await executor.$queryRawUnsafe(
      `SELECT "${schema}".register_post($1, $2::jsonb) AS count`,
      register._def.name,
      jsonData,
    );
    return Number((result as { count: unknown }[])[0]?.count ?? 0);
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Unpost (delete) all movements by recorder.
 */
export async function unpost<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: { $queryRawUnsafe: <T = unknown>(query: string, ...values: unknown[]) => Promise<T> },
  schema: string,
  register: Register<D, R>,
  recorder: string,
): Promise<number> {
  try {
    const result = await executor.$queryRawUnsafe(
      `SELECT "${schema}".register_unpost($1, $2) AS count`,
      register._def.name,
      recorder,
    );
    return Number((result as { count: unknown }[])[0]?.count ?? 0);
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Repost — atomic unpost + post with new data.
 */
export async function repost<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(
  executor: { $queryRawUnsafe: <T = unknown>(query: string, ...values: unknown[]) => Promise<T> },
  schema: string,
  register: Register<D, R>,
  recorder: string,
  data: MovementInput<D, R> | MovementInput<D, R>[],
): Promise<number> {
  const movements = Array.isArray(data) ? data : [data];
  for (const m of movements) {
    validateMovement(m as unknown as Record<string, unknown>, register);
  }

  const jsonData = JSON.stringify(Array.isArray(data) ? data : data);

  try {
    const result = await executor.$queryRawUnsafe(
      `SELECT "${schema}".register_repost($1, $2, $3::jsonb) AS count`,
      register._def.name,
      recorder,
      jsonData,
    );
    return Number((result as { count: unknown }[])[0]?.count ?? 0);
  } catch (err) {
    mapPgError(err);
  }
}
