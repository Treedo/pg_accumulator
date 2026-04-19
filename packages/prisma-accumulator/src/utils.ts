import type { Register } from './types/register';
import { ValidationError } from './errors';

/**
 * Validate register name — alphanumeric + underscore only.
 */
export function validateRegisterName(name: string): void {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new ValidationError(
      `Invalid register name "${name}". Must match [a-zA-Z_][a-zA-Z0-9_]*`,
      'name',
    );
  }
}

/**
 * Validate that required dimension fields are present in the data.
 */
export function validateDimensions(
  data: Record<string, unknown>,
  dimensions: Record<string, string>,
): void {
  for (const key of Object.keys(dimensions)) {
    if (data[key] === undefined || data[key] === null) {
      throw new ValidationError(`Missing required dimension "${key}"`, key);
    }
  }
}

/**
 * Validate movement input data.
 */
export function validateMovement(
  data: Record<string, unknown>,
  register: Register,
): void {
  if (!data.recorder) {
    throw new ValidationError('Movement must have a "recorder" field', 'recorder');
  }
  if (!data.period) {
    throw new ValidationError('Movement must have a "period" field', 'period');
  }
  validateDimensions(data, register._def.dimensions);
}

/**
 * Format a Date or string to ISO string for PostgreSQL.
 */
export function toTimestamp(value: string | Date): string {
  if (value instanceof Date) {
    return value.toISOString();
  }
  return value;
}

/**
 * Sanitize SQL identifier (schema.name) — whitelist approach.
 */
export function sqlIdentifier(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new ValidationError(`Invalid SQL identifier: "${name}"`);
  }
  return `"${name}"`;
}
