import { describe, it, expect, vi } from 'vitest';
import { AccumulatorClient } from '../src/client';
import { defineRegister } from '../src/register';
import { ValidationError } from '../src/errors';

function mockPrisma(response: unknown[] = []) {
  return {
    $queryRawUnsafe: vi.fn().mockResolvedValue(response),
  };
}

const inventory = defineRegister({
  name: 'inventory',
  kind: 'balance',
  dimensions: { warehouse: 'int', product: 'int' },
  resources: { quantity: 'numeric', amount: 'numeric' },
});

describe('AccumulatorClient', () => {
  it('should create with default schema', () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma);
    expect(client).toBeInstanceOf(AccumulatorClient);
  });

  it('should create with custom schema', () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma, { schema: 'my_schema' });
    expect(client).toBeInstanceOf(AccumulatorClient);
  });

  it('should reject invalid register names', () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma);
    expect(() => client.use(defineRegister({
      name: 'DROP TABLE',
      kind: 'balance',
      dimensions: {},
      resources: {},
    }))).toThrow(ValidationError);
  });
});

describe('RegisterHandle.post', () => {
  it('should call register_post with correct SQL', async () => {
    const prisma = mockPrisma([{ count: 1 }]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).post({
      recorder: 'purchase:1',
      period: '2026-04-19',
      warehouse: 1,
      product: 42,
      quantity: 100,
      amount: 5000,
    });

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT "accum".register_post($1, $2::jsonb) AS count',
      'inventory',
      expect.any(String),
    );
  });

  it('should handle batch post', async () => {
    const prisma = mockPrisma([{ count: 2 }]);
    const client = new AccumulatorClient(prisma);

    const result = await client.use(inventory).post([
      { recorder: 'purchase:1', period: '2026-04-19', warehouse: 1, product: 42, quantity: 50, amount: 2500 },
      { recorder: 'purchase:1', period: '2026-04-19', warehouse: 1, product: 43, quantity: 200, amount: 8000 },
    ]);

    expect(result).toBe(2);
  });

  it('should validate movement data', async () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma);

    await expect(
      client.use(inventory).post({
        recorder: 'purchase:1',
        period: '2026-04-19',
        warehouse: 1,
      } as any),
    ).rejects.toThrow(ValidationError);
  });
});

describe('RegisterHandle.unpost', () => {
  it('should call register_unpost', async () => {
    const prisma = mockPrisma([{ count: 3 }]);
    const client = new AccumulatorClient(prisma);

    const result = await client.use(inventory).unpost('purchase:1');

    expect(result).toBe(3);
    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT "accum".register_unpost($1, $2) AS count',
      'inventory',
      'purchase:1',
    );
  });
});

describe('RegisterHandle.repost', () => {
  it('should call register_repost', async () => {
    const prisma = mockPrisma([{ count: 1 }]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).repost('purchase:1', {
      recorder: 'purchase:1',
      period: '2026-04-19',
      warehouse: 1,
      product: 42,
      quantity: 120,
      amount: 6000,
    });

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT "accum".register_repost($1, $2, $3::jsonb) AS count',
      'inventory',
      'purchase:1',
      expect.any(String),
    );
  });
});

describe('RegisterHandle.balance', () => {
  it('should query balance without filters', async () => {
    const prisma = mockPrisma([{ inventory_balance: '{"quantity": 100, "amount": 5000}' }]);
    const client = new AccumulatorClient(prisma);

    const result = await client.use(inventory).balance();

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT * FROM "accum"."inventory_balance"()',
    );
    expect(result).toEqual({ quantity: 100, amount: 5000 });
  });

  it('should query balance with dimensions', async () => {
    const prisma = mockPrisma([{ inventory_balance: '{"quantity": 100, "amount": 5000}' }]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).balance({ warehouse: 1, product: 42 });

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT * FROM "accum"."inventory_balance"(dimensions := $1::jsonb)',
      '{"warehouse":1,"product":42}',
    );
  });

  it('should query balance at date', async () => {
    const prisma = mockPrisma([{ inventory_balance: '{"quantity": 50}' }]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).balance(
      { warehouse: 1 },
      { atDate: '2026-01-01' },
    );

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT * FROM "accum"."inventory_balance"(dimensions := $1::jsonb, at_date := $2::timestamptz)',
      '{"warehouse":1}',
      '2026-01-01',
    );
  });
});

describe('RegisterHandle.turnover', () => {
  it('should query turnover with period and groupBy', async () => {
    const prisma = mockPrisma([
      { inventory_turnover: '{"quantity": 200, "amount": 10000, "warehouse": 1}' },
    ]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).turnover(
      {},
      { dateFrom: '2026-01-01', dateTo: '2026-03-31', groupBy: ['warehouse'] },
    );

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT * FROM "accum"."inventory_turnover"(from_date := $1::timestamptz, to_date := $2::timestamptz, group_by := $3::jsonb)',
      '2026-01-01',
      '2026-03-31',
      '["warehouse"]',
    );
  });
});

describe('RegisterHandle.movements', () => {
  it('should query movements with limit', async () => {
    const prisma = mockPrisma([]);
    const client = new AccumulatorClient(prisma);

    await client.use(inventory).movements(
      { warehouse: 1 },
      { limit: 50 },
    );

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT * FROM "accum"."inventory_movements"(dimensions := $1::jsonb) LIMIT 50',
      '{"warehouse":1}',
    );
  });
});

describe('AccumulatorClient DDL', () => {
  it('should create register', async () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma);

    await client.createRegister(inventory);

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('register_create'),
      'inventory',
      expect.any(String),
      expect.any(String),
      'balance',
    );
  });

  it('should list registers', async () => {
    const prisma = mockPrisma([
      { name: 'inventory', kind: 'balance', dimensions: 2, resources: 2, movements_count: 100, created_at: '2026-01-01' },
    ]);
    const client = new AccumulatorClient(prisma);

    const result = await client.listRegisters();

    expect(result).toHaveLength(1);
    expect(result[0].name).toBe('inventory');
  });

  it('should drop register', async () => {
    const prisma = mockPrisma();
    const client = new AccumulatorClient(prisma);

    await client.dropRegister('inventory', true);

    expect(prisma.$queryRawUnsafe).toHaveBeenCalledWith(
      'SELECT "accum".register_drop($1, $2)',
      'inventory',
      true,
    );
  });
});

describe('withTransaction', () => {
  it('should create a new client with tx executor', async () => {
    const prisma = mockPrisma();
    const tx = mockPrisma([{ count: 1 }]);
    const client = new AccumulatorClient(prisma);
    const txClient = client.withTransaction(tx);

    await txClient.use(inventory).post({
      recorder: 'order:1',
      period: '2026-04-19',
      warehouse: 1,
      product: 42,
      quantity: -5,
      amount: -250,
    });

    // Should use tx, not prisma
    expect(tx.$queryRawUnsafe).toHaveBeenCalled();
    expect(prisma.$queryRawUnsafe).not.toHaveBeenCalled();
  });
});
