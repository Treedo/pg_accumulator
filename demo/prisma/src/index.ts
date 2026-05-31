import express, { Request, Response } from 'express';
import path from 'path';
import { PrismaClient } from '@prisma/client';
import { AccumulatorClient } from 'prisma-accumulator';
import { inventory, generalLedger } from './registers';

const prisma = new PrismaClient();
const accum = new AccumulatorClient(prisma);

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const PORT = process.env.PORT ?? 3303;

const SUPPORTED_LOCALES = ['en', 'uk'];
const TRANSLATIONS: Record<string, Record<string, string>> = {
  en: {
    error_missing_fields: 'Fields warehouse_id, product_id, quantity are required',
    error_recorder_required: 'Field recorder is required',
    error_ledger_required: 'recorder, account_dr, account_cr, amount are required',
  },
  uk: {
    error_missing_fields: "Поля warehouse_id, product_id, quantity обов'язкові",
    error_recorder_required: "Поле recorder обов'язкове",
    error_ledger_required: "recorder, account_dr, account_cr, активна сума обов'язкові",
  },
};

function parseLocale(req: Request) {
  const header = (req.headers['x-lang'] as string) || (req.query.lang as string) || (req.headers['accept-language'] as string) || '';
  const h = (header || '').toLowerCase();
  if (h.startsWith('uk') || h.includes('uk')) return 'uk';
  return 'en';
}

// --- Helpers: product/warehouse names from DB ---
async function getProducts() {
  return prisma.product.findMany({ orderBy: { id: 'asc' } });
}

async function getWarehouses() {
  return prisma.warehouse.findMany({ orderBy: { id: 'asc' } });
}

// --- GET /api/products ---
app.get('/api/products', async (_req: Request, res: Response) => {
  try {
    res.json(await getProducts());
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/warehouses ---
app.get('/api/warehouses', async (_req: Request, res: Response) => {
  try {
    res.json(await getWarehouses());
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/balances ---
app.get('/api/balances', async (_req: Request, res: Response) => {
  try {
    const products = await getProducts();
    const warehouses = await getWarehouses();

    const results: Array<{
      warehouse_id: number;
      warehouse_name: string;
      product_id: number;
      product_name: string;
      quantity: number;
      cost: number;
    }> = [];

    for (const wh of warehouses) {
      for (const prod of products) {
        const bal = await accum.use(inventory).balance({
          warehouse_id: wh.id,
          product_id: prod.id,
        });
        if (bal && (Number(bal.quantity) !== 0 || Number(bal.cost) !== 0)) {
          results.push({
            warehouse_id: wh.id,
            warehouse_name: wh.name,
            product_id: prod.id,
            product_name: prod.name,
            quantity: Number(bal.quantity),
            cost: Number(bal.cost),
          });
        }
      }
    }

    res.json(results);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/movements ---
app.get('/api/movements', async (_req: Request, res: Response) => {
  try {
    const moves = await accum.use(inventory).movements({}, { limit: 50 });
    res.json(moves);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/turnover ---
app.get('/api/turnover', async (req: Request, res: Response) => {
  try {
    const { dateFrom, dateTo, warehouse_id } = req.query as {
      dateFrom?: string;
      dateTo?: string;
      warehouse_id?: string;
    };

    const dims: Record<string, number> = {};
    if (warehouse_id) dims.warehouse_id = Number(warehouse_id);

    const turn = await accum.use(inventory).turnover(dims, {
      dateFrom: dateFrom || '2020-01-01',
      dateTo: dateTo || '2030-12-31',
      groupBy: ['product_id'],
    });
    res.json(turn);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/receipt — goods receipt (positive) ---
app.post('/api/receipt', async (req: Request, res: Response) => {
  const { warehouse_id, product_id, quantity, cost } = req.body as {
    warehouse_id: number;
    product_id: number;
    quantity: number;
    cost: number;
  };

  if (!warehouse_id || !product_id || !quantity) {
    res.status(400).json({ error: TRANSLATIONS[parseLocale(req)].error_missing_fields });
    return;
  }

  try {
    const recorder = `receipt:${Date.now()}`;
    const period = new Date().toISOString().slice(0, 10);

    await accum.use(inventory).post({
      recorder,
      period,
      warehouse_id,
      product_id,
      quantity: Math.abs(quantity),
      cost: Math.abs(cost || 0),
    });

    res.json({ ok: true, recorder });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/shipment — goods shipment (negative) ---
app.post('/api/shipment', async (req: Request, res: Response) => {
  const { warehouse_id, product_id, quantity, cost } = req.body as {
    warehouse_id: number;
    product_id: number;
    quantity: number;
    cost: number;
  };

  if (!warehouse_id || !product_id || !quantity) {
    res.status(400).json({ error: TRANSLATIONS[parseLocale(req)].error_missing_fields });
    return;
  }

  try {
    const recorder = `shipment:${Date.now()}`;
    const period = new Date().toISOString().slice(0, 10);

    await accum.use(inventory).post({
      recorder,
      period,
      warehouse_id,
      product_id,
      quantity: -Math.abs(quantity),
      cost: -Math.abs(cost || 0),
    });

    res.json({ ok: true, recorder });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/unpost — cancel operation ---
app.post('/api/unpost', async (req: Request, res: Response) => {
  const { recorder } = req.body as { recorder: string };

  if (!recorder) {
    res.status(400).json({ error: TRANSLATIONS[parseLocale(req)].error_recorder_required });
    return;
  }

  try {
    const deleted = await accum.use(inventory).unpost(recorder);
    res.json({ ok: true, deleted });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- General Ledger Endpoints ---

app.get('/api/ledger/balances', async (_req: Request, res: Response) => {
  try {
    const result = await prisma.$queryRawUnsafe<any[]>(`
      SELECT 
        account, 
        subconto, 
        amount_dr, 
        amount_cr, 
        currency,
        CASE 
          WHEN account ~ '^(1|2|9)' THEN amount_dr - amount_cr
          WHEN account ~ '^(4|5|7)' THEN amount_cr - amount_dr
          ELSE amount_dr - amount_cr
        END as balance,
        CASE
          WHEN account ~ '^(1|2|9)' THEN 'A'
          WHEN account ~ '^(4|5|7)' THEN 'P'
          ELSE 'AP'
        END as acc_type
      FROM accum.general_ledger_balance_cache
      ORDER BY account, subconto
    `);
    
    // cast numbers explicitly since prisma may return Decimal/BigInt
    const formatted = result.map(row => ({
      account: row.account,
      subconto: row.subconto,
      amount_dr: Number(row.amount_dr),
      amount_cr: Number(row.amount_cr),
      balance: Number(row.balance),
      currency: row.currency,
      acc_type: row.acc_type,
    }));
    
    res.json(formatted);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

app.get('/api/ledger/movements', async (_req: Request, res: Response) => {
  try {
    const moves = await accum.use(generalLedger).movements({}, { limit: 50 });
    res.json(moves);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

app.get('/api/ledger/verify', async (_req: Request, res: Response) => {
  try {
    const result = await prisma.$queryRawUnsafe<any[]>(`
      SELECT accum.register_ledger_verify('general_ledger') as sound
    `);
    const sound = result[0]?.sound ?? true;
    res.json({ sound: Boolean(sound) });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

app.post('/api/ledger/post', async (req: Request, res: Response) => {
  const { recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, amount, currency } = req.body;
  if (!recorder || !account_dr || !account_cr || amount === undefined) {
    res.status(400).json({ error: TRANSLATIONS[parseLocale(req)].error_ledger_required });
    return;
  }
  try {
    let sDr = subconto_dr;
    if (typeof sDr === 'string' && sDr.trim()) {
      try { sDr = JSON.parse(sDr); } catch(e) { sDr = { name: sDr }; }
    } else if (!sDr) {
      sDr = {};
    }
    
    let sCr = subconto_cr;
    if (typeof sCr === 'string' && sCr.trim()) {
      try { sCr = JSON.parse(sCr); } catch(e) { sCr = { name: sCr }; }
    } else if (!sCr) {
      sCr = {};
    }

    const count = await accum.use(generalLedger).post({
      recorder,
      period: period || new Date().toISOString().slice(0, 10),
      currency: currency || 'USD',
      account_dr,
      subconto_dr: sDr,
      account_cr,
      subconto_cr: sCr,
      amount: Number(amount),
    });
    res.json({ ok: true, count });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

app.post('/api/ledger/unpost', async (req: Request, res: Response) => {
  const { recorder } = req.body;
  if (!recorder) {
    res.status(400).json({ error: TRANSLATIONS[parseLocale(req)].error_recorder_required });
    return;
  }
  try {
    const deleted = await accum.use(generalLedger).unpost(recorder);
    res.json({ ok: true, deleted });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/registers — list all registers ---
app.get('/api/registers', async (_req: Request, res: Response) => {
  try {
    const list = await accum.listRegisters();
    res.json(list);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Prisma-accumulator demo running at http://localhost:${PORT}`);
});
