import { Pool } from 'pg';

export const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    'postgresql://dev:dev_password@postgres:5432/accumulator_dev',
});
