/**
 * Custom error classes for prisma-accumulator.
 */

export class AccumulatorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AccumulatorError';
  }
}

export class RegisterNotFoundError extends AccumulatorError {
  public readonly registerName: string;

  constructor(name: string) {
    super(`Register "${name}" not found`);
    this.name = 'RegisterNotFoundError';
    this.registerName = name;
  }
}

export class RecorderNotFoundError extends AccumulatorError {
  public readonly recorder: string;

  constructor(recorder: string) {
    super(`Recorder "${recorder}" not found`);
    this.name = 'RecorderNotFoundError';
    this.recorder = recorder;
  }
}

export class ValidationError extends AccumulatorError {
  public readonly field?: string;

  constructor(message: string, field?: string) {
    super(message);
    this.name = 'ValidationError';
    this.field = field;
  }
}

/**
 * Map PostgreSQL RAISE EXCEPTION codes to typed errors.
 */
export function mapPgError(err: unknown): never {
  if (err instanceof Error) {
    const msg = err.message;
    if (msg.includes('register') && (msg.includes('not found') || msg.includes('does not exist'))) {
      const match = msg.match(/register\s+"?([^"]+)"?/i);
      throw new RegisterNotFoundError(match?.[1] ?? 'unknown');
    }
    if (msg.includes('recorder') && msg.includes('not found')) {
      const match = msg.match(/recorder\s+"?([^"]+)"?/i);
      throw new RecorderNotFoundError(match?.[1] ?? 'unknown');
    }
  }
  throw err;
}
