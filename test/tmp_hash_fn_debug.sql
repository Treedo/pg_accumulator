CREATE EXTENSION IF NOT EXISTS pg_accumulator;
SELECT accum.register_create(name := 'sales_x', dimensions := '{"product": "int", "region": "text"}'::jsonb, resources := '{"sold_qty": "numeric", "revenue": "numeric(18,2)"}'::jsonb, kind := 'turnover');
SELECT p.oid::regprocedure::text AS signature, pg_get_functiondef(p.oid) AS def
FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'accum' AND p.proname = '_hash_sales_x';
