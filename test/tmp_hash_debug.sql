CREATE EXTENSION IF NOT EXISTS pg_accumulator;
SELECT * FROM jsonb_each_text('{"product": "int", "region": "text"}'::jsonb) ORDER BY key;
