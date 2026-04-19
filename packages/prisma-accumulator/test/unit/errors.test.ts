import { describe, it, expect } from 'vitest';
import {
  AccumulatorError,
  RegisterNotFoundError,
  RecorderNotFoundError,
  ValidationError,
  mapPgError,
} from '../src/errors';

describe('Error classes', () => {
  it('should create AccumulatorError', () => {
    const err = new AccumulatorError('test');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('AccumulatorError');
  });

  it('should create RegisterNotFoundError', () => {
    const err = new RegisterNotFoundError('inventory');
    expect(err).toBeInstanceOf(AccumulatorError);
    expect(err.registerName).toBe('inventory');
    expect(err.message).toContain('inventory');
  });

  it('should create RecorderNotFoundError', () => {
    const err = new RecorderNotFoundError('txn:123');
    expect(err).toBeInstanceOf(AccumulatorError);
    expect(err.recorder).toBe('txn:123');
  });

  it('should create ValidationError with field', () => {
    const err = new ValidationError('bad value', 'name');
    expect(err.field).toBe('name');
  });
});

describe('mapPgError', () => {
  it('should map register not found errors', () => {
    const pgErr = new Error('register "inventory" not found');
    expect(() => mapPgError(pgErr)).toThrow(RegisterNotFoundError);
  });

  it('should map recorder not found errors', () => {
    const pgErr = new Error('recorder "txn:123" not found');
    expect(() => mapPgError(pgErr)).toThrow(RecorderNotFoundError);
  });

  it('should re-throw unknown errors', () => {
    const pgErr = new Error('something else');
    expect(() => mapPgError(pgErr)).toThrow('something else');
  });
});
