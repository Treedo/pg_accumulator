import type { Register, RegisterDefinition } from './types/register';

/**
 * Define a typed accumulation register.
 * Returns a register handle used with AccumulatorClient.
 */
export function defineRegister<
  D extends Record<string, string>,
  R extends Record<string, string>,
>(def: RegisterDefinition<D, R>): Register<D, R> {
  return { _def: def };
}
