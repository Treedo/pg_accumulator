import express, { Request, Response } from 'express';
import path from 'path';
import { pool } from './db';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const PORT = process.env.PORT ?? 3302;

const ACCOUNTS: Record<number, string> = {
  1: 'Готівка',
  2: 'Банківський рахунок',
  3: 'Кредитна картка',
};

const CATEGORIES: Record<number, string> = {
  1: 'Зарплата',
  2: 'Їжа',
  3: 'Транспорт',
  4: 'Комунальні',
  5: 'Розваги',
  6: 'Інше',
};

function accountName(id: number): string {
  return ACCOUNTS[id] ?? `Рахунок #${id}`;
}

function categoryName(id: number): string {
  return CATEGORIES[id] ?? `Категорія #${id}`;
}

// --- GET /api/balances — balance_cache grouped by account ---
app.get('/api/balances', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query<{ account: number; total: string }>(`
      SELECT account, SUM(amount) AS total
      FROM accum.finance_balance_cache
      GROUP BY account
      ORDER BY account
    `);
    res.json(
      rows.map((r) => ({
        account: r.account,
        account_name: accountName(r.account),
        total: r.total,
      }))
    );
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/movements — recent 50 movements ---
app.get('/api/movements', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query<{
      id: number;
      recorder: string;
      period: Date;
      account: number;
      category: number;
      amount: string;
      recorded_at: Date;
    }>(`
      SELECT id, recorder, period, account, category, amount, recorded_at
      FROM accum.finance_movements
      ORDER BY recorded_at DESC, id DESC
      LIMIT 50
    `);
    res.json(
      rows.map((r) => ({
        ...r,
        period: r.period?.toISOString().slice(0, 10) ?? null,
        recorded_at: r.recorded_at?.toISOString() ?? null,
        account_name: accountName(r.account),
        category_name: categoryName(r.category),
      }))
    );
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/post ---
app.post('/api/post', async (req: Request, res: Response) => {
  const { recorder, period, account, category, amount } = req.body as {
    recorder: string;
    period: string;
    account: number;
    category: number;
    amount: number;
  };

  if (!recorder || !period || !account || !category || amount === undefined) {
    res.status(400).json({ error: "Усі поля обов'язкові" });
    return;
  }

  try {
    await pool.query('SELECT accum.register_post($1, $2::jsonb)', [
      'finance',
      JSON.stringify({
        recorder,
        period,
        account: Number(account),
        category: Number(category),
        amount: Number(amount),
      }),
    ]);
    res.json({ ok: true });
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/unpost ---
app.post('/api/unpost', async (req: Request, res: Response) => {
  const { recorder } = req.body as { recorder: string };

  if (!recorder) {
    res.status(400).json({ error: 'recorder є обов\'язковим' });
    return;
  }

  try {
    await pool.query('SELECT accum.register_unpost($1, $2)', ['finance', recorder]);
    res.json({ ok: true });
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/balance — point-in-time balance query ---
app.get('/api/balance', async (req: Request, res: Response) => {
  const { account, category, at_date } = req.query as Record<string, string>;

  const dims: Record<string, number> = {};
  if (account) dims['account'] = Number(account);
  if (category) dims['category'] = Number(category);

  try {
    let query: string;
    let params: unknown[];

    if (at_date) {
      query =
        "SELECT * FROM accum.finance_balance(dimensions := $1::jsonb, at_date := $2::timestamptz)";
      params = [JSON.stringify(dims), at_date];
    } else {
      query = 'SELECT * FROM accum.finance_balance(dimensions := $1::jsonb)';
      params = [JSON.stringify(dims)];
    }

    const { rows } = await pool.query(query, params);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/turnover ---
app.get('/api/turnover', async (req: Request, res: Response) => {
  const { account, category, date_from, date_to } = req.query as Record<string, string>;

  const dims: Record<string, number> = {};
  if (account) dims['account'] = Number(account);
  if (category) dims['category'] = Number(category);

  try {
    let query = 'SELECT * FROM accum.finance_turnover(dimensions := $1::jsonb';
    const params: unknown[] = [JSON.stringify(dims)];

    if (date_from) {
      query += ', date_from := $2::timestamptz';
      params.push(date_from);
    }
    if (date_to) {
      query += `, date_to := $${params.length + 1}::timestamptz`;
      params.push(date_to);
    }
    query += ')';

    const { rows } = await pool.query(query, params);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/meta — labels for dropdowns ---
app.get('/api/meta', (_req: Request, res: Response) => {
  res.json({ accounts: ACCOUNTS, categories: CATEGORIES });
});

app.listen(PORT, () => {
  console.log(`pg_accumulator TS demo → http://localhost:${PORT}`);
});
