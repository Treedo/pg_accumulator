import type {
  Register,
  AccumulatorConfig,
  MovementInput,
  BalanceOptions,
  TurnoverOptions,
  MovementsOptions,
  ResourceResult,
  TurnoverRow,
  MovementRow,
  RegisterInfo,
  RegisterListRow,
  AlterRegisterOptions,
} from './types/register';
import type { DimensionFilter } from './types/pg-types';
import { post, unpost, repost } from './operations/post';
import { balance, turnover, movements } from './operations/read';
import {
  createRegister,
  alterRegister,
  dropRegister,
  listRegisters,
  registerInfo,
} from './operations/ddl';
import { validateRegisterName } from './utils';

/** Minimal Prisma client interface required */
interface PrismaLike {
  $queryRawUnsafe: (...args: unknown[]) => Promise<unknown[]>;
}

/**
 * A register handle bound to a specific register for chained operations.
 */
export class RegisterHandle<
  D extends Record<string, string>,
  R extends Record<string, string>,
> {
  constructor(
    private readonly executor: PrismaLike,
    private readonly schema: string,
    private readonly register: Register<D, R>,
  ) {}

  /** Post one or more movements */
  async post(data: MovementInput<D, R> | MovementInput<D, R>[]): Promise<number> {
    return post(this.executor, this.schema, this.register, data);
  }

  /** Unpost all movements by recorder */
  async unpost(recorder: string): Promise<number> {
    return unpost(this.executor, this.schema, this.register, recorder);
  }

  /** Repost — atomic unpost + post with new data */
  async repost(recorder: string, data: MovementInput<D, R> | MovementInput<D, R>[]): Promise<number> {
    return repost(this.executor, this.schema, this.register, recorder, data);
  }

  /** Query balance (current or historical) */
  async balance(
    dims?: DimensionFilter<D>,
    options?: BalanceOptions,
  ): Promise<ResourceResult<R> | null> {
    return balance(this.executor, this.schema, this.register, dims, options);
  }

  /** Query turnover for a period */
  async turnover(
    dims?: DimensionFilter<D>,
    options?: TurnoverOptions,
  ): Promise<TurnoverRow<R>[]> {
    return turnover(this.executor, this.schema, this.register, dims, options);
  }

  /** Query movements with filters */
  async movements(
    dims?: DimensionFilter<D>,
    options?: MovementsOptions,
  ): Promise<MovementRow<D, R>[]> {
    return movements(this.executor, this.schema, this.register, dims, options);
  }
}

/**
 * Type-safe client for pg_accumulator, designed to work alongside Prisma.
 */
export class AccumulatorClient {
  private readonly schema: string;

  constructor(
    private readonly prisma: PrismaLike,
    config?: AccumulatorConfig,
  ) {
    this.schema = config?.schema ?? 'accum';
  }

  /**
   * Select a register for operations.
   * Returns a RegisterHandle with type-safe post/unpost/balance/turnover/movements.
   */
  use<D extends Record<string, string>, R extends Record<string, string>>(
    register: Register<D, R>,
  ): RegisterHandle<D, R> {
    validateRegisterName(register._def.name);
    return new RegisterHandle(this.prisma, this.schema, register);
  }

  /**
   * Create a register in the database (DDL).
   */
  async createRegister(register: Register): Promise<void> {
    validateRegisterName(register._def.name);
    return createRegister(this.prisma, this.schema, register);
  }

  /**
   * Alter an existing register.
   */
  async alterRegister(name: string, options: AlterRegisterOptions): Promise<void> {
    validateRegisterName(name);
    return alterRegister(this.prisma, this.schema, name, options);
  }

  /**
   * Drop a register.
   */
  async dropRegister(name: string, force = false): Promise<void> {
    validateRegisterName(name);
    return dropRegister(this.prisma, this.schema, name, force);
  }

  /**
   * List all registers.
   */
  async listRegisters(): Promise<RegisterListRow[]> {
    return listRegisters(this.prisma, this.schema);
  }

  /**
   * Get detailed info about a register.
   */
  async registerInfo(name: string): Promise<RegisterInfo> {
    validateRegisterName(name);
    return registerInfo(this.prisma, this.schema, name);
  }

  /**
   * Create a new AccumulatorClient bound to a Prisma transaction client.
   * Use inside prisma.$transaction() for transactional operations.
   */
  withTransaction(tx: PrismaLike): AccumulatorClient {
    return new AccumulatorClient(tx, { schema: this.schema });
  }
}
