import { describe, it, expect } from 'vitest';
import {
  validateRegisterName,
  validateDimensions,
  validateMovement,
  toTimestamp,
  sqlIdentifier,
} from '../src/utils';
import { defineRegister } from '../src/register';
import { ValidationError } from '../src/errors';

describe('validateRegisterName', () => {
  it('should accept valid names', () => {
    expect(() => validateRegisterName('inventory')).not.toThrow();
    expect(() => validateRegisterName('my_register_1')).not.toThrow();
    expect(() => validateRegisterName('_private')).not.toThrow();
  });

  it('should reject invalid names', () => {
    expect(() => validateRegisterName('123abc')).toThrow(ValidationError);
    expect(() => validateRegisterName('my-register')).toThrow(ValidationError);
    expect(() => validateRegisterName('DROP TABLE')).toThrow(ValidationError);
    expect(() => validateRegisterName('')).toThrow(ValidationError);
    expect(() => validateRegisterName('a;b')).toThrow(ValidationError);
  });
});

describe('validateDimensions', () => {
  it('should pass when all dimensions present', () => {
    expect(() =>
      validateDimensions(
        { warehouse: 1, product: 42 },
        { warehouse: 'int', product: 'int' },
      ),
    ).not.toThrow();
  });

  it('should throw on missing dimension', () => {
    expect(() =>
      validateDimensions(
        { warehouse: 1 },
        { warehouse: 'int', product: 'int' },
      ),
    ).toThrow(ValidationError);
  });
});

describe('validateMovement', () => {
  const reg = defineRegister({
    name: 'test',
    kind: 'balance',
    dimensions: { account: 'int' },
    resources: { amount: 'numeric' },
  });

  it('should accept valid movement', () => {
    expect(() =>
      validateMovement(
        { recorder: 'txn:1', period: '2026-01-01', account: 1, amount: 100 },
        reg,
      ),
    ).not.toThrow();
  });

  it('should throw on missing recorder', () => {
    expect(() =>
      validateMovement(
        { period: '2026-01-01', account: 1, amount: 100 },
        reg,
      ),
    ).toThrow(ValidationError);
  });

  it('should throw on missing period', () => {
    expect(() =>
      validateMovement(
        { recorder: 'txn:1', account: 1, amount: 100 },
        reg,
      ),
    ).toThrow(ValidationError);
  });

  it('should throw on missing dimension', () => {
    expect(() =>
      validateMovement(
        { recorder: 'txn:1', period: '2026-01-01', amount: 100 },
        reg,
      ),
    ).toThrow(ValidationError);
  });
});

describe('toTimestamp', () => {
  it('should return string as-is', () => {
    expect(toTimestamp('2026-01-01')).toBe('2026-01-01');
  });

  it('should convert Date to ISO string', () => {
    const d = new Date('2026-01-01T00:00:00Z');
    expect(toTimestamp(d)).toBe('2026-01-01T00:00:00.000Z');
  });
});

describe('sqlIdentifier', () => {
  it('should quote valid identifiers', () => {
    expect(sqlIdentifier('my_table')).toBe('"my_table"');
  });

  it('should reject invalid identifiers', () => {
    expect(() => sqlIdentifier('DROP TABLE')).toThrow(ValidationError);
    expect(() => sqlIdentifier("'; --")).toThrow(ValidationError);
  });
});
