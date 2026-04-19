// Public API
export { AccumulatorClient, RegisterHandle } from './client';
export { defineRegister } from './register';

// Errors
export {
  AccumulatorError,
  RegisterNotFoundError,
  RecorderNotFoundError,
  ValidationError,
} from './errors';

// Types
export type {
  RegisterKind,
  TotalsPeriod,
  PartitionBy,
  RecorderType,
  RegisterDefinition,
  Register,
  MovementInput,
  BalanceOptions,
  TurnoverOptions,
  MovementsOptions,
  TurnoverRow,
  MovementRow,
  RegisterInfo,
  RegisterListRow,
  AlterRegisterOptions,
  AccumulatorConfig,
} from './types';

export type {
  PgColumnType,
  PgToTs,
  MapPgFields,
  DimensionFilter,
  ResourceResult,
} from './types';
