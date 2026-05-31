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

const LEDGER_ACCOUNTS: Record<string, string> = {
  '10': '10 Готівка та Рахунки (Активний)',
  '28': '28 Товари/Склад (Активний)',
  '90': '90 Витрати на оренду (Активний)',
  '40': '40 Статутний капітал (Пасивний)',
  '50': '50 Кредити банку (Пасивний)',
};

function accountName(id: number): string {
  return ACCOUNTS[id] ?? `Рахунок #${id}`;
}

function categoryName(id: number): string {
  return CATEGORIES[id] ?? `Категорія #${id}`;
}

// --- GET /api/registers — Compatibility registers list ---
app.get('/api/registers', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query('SELECT name, kind, dimensions, resources FROM accum.registers ORDER BY name');
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

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
  const { register, recorder } = req.body as { register?: string; recorder: string };

  if (!recorder) {
    res.status(400).json({ error: 'recorder є обов\'язковим' });
    return;
  }

  const regName = register || 'finance';

  try {
    await pool.query('SELECT accum.register_unpost($1, $2)', [regName, recorder]);
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

// --- GET /api/ledger/balances — balance_cache for ledger ---
app.get('/api/ledger/balances', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query(`
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
            WHEN account ~ '^(1|2|9)' THEN 'A' -- Active (Asset / Expense)
            WHEN account ~ '^(4|5|7)' THEN 'P' -- Passive (Liabilities / Equity)
            ELSE 'AP'
        END as acc_type
      FROM accum.general_ledger_balance_cache
      ORDER BY account, subconto
    `);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/ledger/movements — ledger recent movements ---
app.get('/api/ledger/movements', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query(`
      SELECT id, recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount, recorded_at
      FROM accum.general_ledger_movements
      ORDER BY recorded_at DESC, id DESC
      LIMIT 50
    `);
    res.json(
      rows.map((r) => ({
        ...r,
        period: r.period?.toISOString().slice(0, 10) ?? null,
        recorded_at: r.recorded_at?.toISOString() ?? null,
      }))
    );
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/ledger/verify — verify general_ledger soundness ---
app.get('/api/ledger/verify', async (_req: Request, res: Response) => {
  try {
    const { rows } = await pool.query("SELECT accum.register_ledger_verify('general_ledger') as sound");
    const sound = rows[0]?.sound ?? false;
    res.json({ sound });
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- POST /api/ledger/post ---
app.post('/api/ledger/post', async (req: Request, res: Response) => {
  const { recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount } = req.body as {
    recorder: string;
    period: string;
    account_dr: string;
    subconto_dr: string | Record<string, any>;
    account_cr: string;
    subconto_cr: string | Record<string, any>;
    currency: string;
    amount: number;
  };

  if (!recorder || !period || !account_dr || !account_cr || amount === undefined) {
    res.status(400).json({ error: "Усі поля обов'язкові" });
    return;
  }

  // Parse subconto if string
  let parsedDr: Record<string, any> = {};
  if (typeof subconto_dr === 'string') {
    const trimmed = subconto_dr.trim();
    if (trimmed.startsWith('{')) {
      try { parsedDr = JSON.parse(trimmed); } catch { parsedDr = { note: trimmed }; }
    } else if (trimmed) {
      parsedDr = { name: trimmed };
    }
  } else if (subconto_dr) {
    parsedDr = subconto_dr;
  }

  let parsedCr: Record<string, any> = {};
  if (typeof subconto_cr === 'string') {
    const trimmed = subconto_cr.trim();
    if (trimmed.startsWith('{')) {
      try { parsedCr = JSON.parse(trimmed); } catch { parsedCr = { note: trimmed }; }
    } else if (trimmed) {
      parsedCr = { name: trimmed };
    }
  } else if (subconto_cr) {
    parsedCr = subconto_cr;
  }

  try {
    await pool.query('SELECT accum.register_post($1, $2::jsonb)', [
      'general_ledger',
      JSON.stringify({
        recorder,
        period,
        account_dr,
        subconto_dr: parsedDr,
        account_cr,
        subconto_cr: parsedCr,
        currency: currency || 'USD',
        amount: Number(amount),
      }),
    ]);
    res.json({ ok: true });
  } catch (e: unknown) {
    res.status(500).json({ error: String(e) });
  }
});

// --- GET /api/meta ---
app.get('/api/meta', (_req: Request, res: Response) => {
  res.json({ accounts: ACCOUNTS, categories: CATEGORIES, ledgerAccounts: LEDGER_ACCOUNTS });
});

async function bootstrap() {
  try {
    // Ensure view exists
    await pool.query(`
      CREATE OR REPLACE VIEW accum.registers AS
      SELECT name, kind, dimensions, resources, totals_period, partition_by, high_write, created_at, updated_at
      FROM accum._registers;
    `).catch(() => {});

    // Ensure 'finance' register exists
    const financeCheck = await pool.query("SELECT name FROM accum._registers WHERE name = 'finance'");
    if (financeCheck.rows.length === 0) {
      console.log("Registering 'finance' automatically at startup...");
      await pool.query(`
        SELECT accum.register_create(
            name          := 'finance',
            dimensions    := '{"account": "int", "category": "int"}',
            resources     := '{"amount": "numeric(18,2)"}',
            kind          := 'balance',
            totals_period := 'month'
        );
      `);
      // Seed finance
      await pool.query(`
        SELECT accum.register_post('finance', '{"recorder": "income:001", "period": "2026-04-01", "account": 2, "category": 1, "amount": 55000}');
        SELECT accum.register_post('finance', '{"recorder": "income:002", "period": "2026-04-01", "account": 1, "category": 1, "amount": 15000}');
        SELECT accum.register_post('finance', '{"recorder": "expense:001", "period": "2026-04-03", "account": 1, "category": 2, "amount": -2400}');
        SELECT accum.register_post('finance', '{"recorder": "expense:002", "period": "2026-04-10", "account": 2, "category": 2, "amount": -3100}');
        SELECT accum.register_post('finance', '{"recorder": "expense:003", "period": "2026-04-17", "account": 1, "category": 2, "amount": -1800}');
        SELECT accum.register_post('finance', '{"recorder": "expense:004", "period": "2026-04-05", "account": 2, "category": 3, "amount": -850}');
        SELECT accum.register_post('finance', '{"recorder": "expense:005", "period": "2026-04-08", "account": 2, "category": 4, "amount": -3400}');
        SELECT accum.register_post('finance', '{"recorder": "expense:006", "period": "2026-04-14", "account": 3, "category": 5, "amount": -4200}');
        SELECT accum.register_post('finance', '{"recorder": "expense:007", "period": "2026-04-18", "account": 3, "category": 5, "amount": -1500}');
      `);
      console.log("'finance' registered and seeded successfully.");
    }

    // Ensure 'general_ledger' register exists
    const ledgerCheck = await pool.query("SELECT name FROM accum._registers WHERE name = 'general_ledger'");
    if (ledgerCheck.rows.length === 0) {
      console.log("Registering 'general_ledger' automatically at startup...");
      await pool.query(`
        SELECT accum.register_create(
            name          := 'general_ledger',
            dimensions    := '{"currency": "text"}',
            resources     := '{"amount": "numeric(18,2)"}',
            kind          := 'ledger',
            totals_period := 'day'
        );
      `);
      
      console.log("Seeding initial ledger transactions...");
      await pool.query(`
        SELECT accum.register_post('general_ledger', '{
            "recorder": "capital:1",
            "period":   "2026-04-01",
            "currency": "USD",
            "account_dr": "10",
            "subconto_dr": {"bank": "Головний банк"},
            "account_cr": "40",
            "subconto_cr": {"owner": "Засновник"},
            "amount": 100000.00
        }');
      `);
      await pool.query(`
        SELECT accum.register_post('general_ledger', '{
            "recorder": "purchase:1",
            "period":   "2026-04-02",
            "currency": "USD",
            "account_dr": "28",
            "subconto_dr": {"item_id": 1, "supplier": "Оптовий постачальник"},
            "account_cr": "10",
            "subconto_cr": {"bank": "Головний банк"},
            "amount": 30000.00
        }');
      `);
      await pool.query(`
        SELECT accum.register_post('general_ledger', '{
            "recorder": "rent:1",
            "period":   "2026-04-05",
            "currency": "USD",
            "account_dr": "90",
            "subconto_dr": {"purpose": "Оренда офісу за Квітень"},
            "account_cr": "10",
            "subconto_cr": {"bank": "Головний банк"},
            "amount": 2000.00
        }');
      `);
      await pool.query(`
        SELECT accum.register_post('general_ledger', '{
            "recorder": "loan:1",
            "period":   "2026-04-06",
            "currency": "USD",
            "account_dr": "10",
            "subconto_dr": {"bank": "Головний банк"},
            "account_cr": "50",
            "subconto_cr": {"lender": "Альфа Банк"},
            "amount": 50000.00
        }');
      `);
      console.log("'general_ledger' registered and seeded successfully.");
    }
  } catch (error) {
    console.error("Database initialization error during bootstrap:", error);
  }
}

bootstrap().then(() => {
  app.listen(PORT, () => {
    console.log(`pg_accumulator TS demo → http://localhost:${PORT}`);
  });
});
