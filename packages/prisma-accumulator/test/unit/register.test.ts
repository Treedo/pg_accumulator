import { describe, it, expect } from 'vitest';
import { defineRegister } from '../src/register';

describe('defineRegister', () => {
  it('should create a register definition with required fields', () => {
    const reg = defineRegister({
      name: 'inventory',
      kind: 'balance',
      dimensions: { warehouse: 'int', product: 'int' },
      resources: { quantity: 'numeric', amount: 'numeric' },
    });

    expect(reg._def.name).toBe('inventory');
    expect(reg._def.kind).toBe('balance');
    expect(reg._def.dimensions).toEqual({ warehouse: 'int', product: 'int' });
    expect(reg._def.resources).toEqual({ quantity: 'numeric', amount: 'numeric' });
  });

  it('should preserve optional fields', () => {
    const reg = defineRegister({
      name: 'sales',
      kind: 'turnover',
      dimensions: { customer: 'int' },
      resources: { revenue: 'numeric' },
      totals_period: 'month',
      partition_by: 'quarter',
      high_write: true,
      recorder_type: 'uuid',
    });

    expect(reg._def.totals_period).toBe('month');
    expect(reg._def.partition_by).toBe('quarter');
    expect(reg._def.high_write).toBe(true);
    expect(reg._def.recorder_type).toBe('uuid');
  });
});
