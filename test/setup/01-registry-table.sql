-- test/setup/01-registry-table.sql
-- Internal metadata registry for accumulation registers

CREATE TABLE IF NOT EXISTS accum._registers (
    name           text PRIMARY KEY,
    kind           text NOT NULL DEFAULT 'balance'
                       CHECK (kind IN ('balance', 'turnover')),
    dimensions     jsonb NOT NULL,
    resources      jsonb NOT NULL,
    totals_period  text NOT NULL DEFAULT 'day'
                       CHECK (totals_period IN ('day', 'month', 'year')),
    partition_by   text NOT NULL DEFAULT 'month'
                       CHECK (partition_by IN ('day', 'month', 'quarter', 'year')),
    high_write     boolean NOT NULL DEFAULT false,
    recorder_type  text NOT NULL DEFAULT 'text',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
