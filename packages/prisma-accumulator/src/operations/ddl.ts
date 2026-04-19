import type { Register, RegisterInfo, RegisterListRow, AlterRegisterOptions } from '../types/register';
import { mapPgError } from '../errors';

type Executor = { $queryRawUnsafe: (...args: unknown[]) => Promise<unknown[]> };

/**
 * Create a register in the database.
 */
export async function createRegister(
  executor: Executor,
  schema: string,
  register: Register,
): Promise<void> {
  const def = register._def;
  const params: unknown[] = [
    def.name,
    JSON.stringify(def.dimensions),
    JSON.stringify(def.resources),
    def.kind,
  ];
  const parts = [
    `name := $1`,
    `dimensions := $2::jsonb`,
    `resources := $3::jsonb`,
    `kind := $4`,
  ];
  let idx = 5;

  if (def.totals_period) {
    parts.push(`totals_period := $${idx}`);
    params.push(def.totals_period);
    idx++;
  }
  if (def.partition_by) {
    parts.push(`partition_by := $${idx}`);
    params.push(def.partition_by);
    idx++;
  }
  if (def.high_write !== undefined) {
    parts.push(`high_write := $${idx}`);
    params.push(def.high_write);
    idx++;
  }
  if (def.recorder_type) {
    parts.push(`recorder_type := $${idx}`);
    params.push(def.recorder_type);
    idx++;
  }

  const sql = `SELECT "${schema}".register_create(${parts.join(', ')})`;

  try {
    await executor.$queryRawUnsafe(sql, ...params);
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Alter an existing register.
 */
export async function alterRegister(
  executor: Executor,
  schema: string,
  name: string,
  options: AlterRegisterOptions,
): Promise<void> {
  const params: unknown[] = [name];
  const parts = [`p_name := $1`];
  let idx = 2;

  if (options.addDimensions) {
    parts.push(`add_dimensions := $${idx}::jsonb`);
    params.push(JSON.stringify(options.addDimensions));
    idx++;
  }
  if (options.addResources) {
    parts.push(`add_resources := $${idx}::jsonb`);
    params.push(JSON.stringify(options.addResources));
    idx++;
  }
  if (options.highWrite !== undefined) {
    parts.push(`high_write := $${idx}`);
    params.push(options.highWrite);
    idx++;
  }

  const sql = `SELECT "${schema}".register_alter(${parts.join(', ')})`;

  try {
    await executor.$queryRawUnsafe(sql, ...params);
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Drop a register.
 */
export async function dropRegister(
  executor: Executor,
  schema: string,
  name: string,
  force = false,
): Promise<void> {
  try {
    await executor.$queryRawUnsafe(
      `SELECT "${schema}".register_drop($1, $2)`,
      name,
      force,
    );
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * List all registers.
 */
export async function listRegisters(
  executor: Executor,
  schema: string,
): Promise<RegisterListRow[]> {
  try {
    const rows = await executor.$queryRawUnsafe(
      `SELECT * FROM "${schema}".register_list()`,
    );
    return rows as RegisterListRow[];
  } catch (err) {
    mapPgError(err);
  }
}

/**
 * Get detailed info about a register.
 */
export async function registerInfo(
  executor: Executor,
  schema: string,
  name: string,
): Promise<RegisterInfo> {
  try {
    const rows = await executor.$queryRawUnsafe(
      `SELECT "${schema}".register_info($1) AS info`,
      name,
    );
    const row = (rows as { info: unknown }[])[0];
    const info = row?.info;
    if (typeof info === 'string') return JSON.parse(info);
    return info as RegisterInfo;
  } catch (err) {
    mapPgError(err);
  }
}
