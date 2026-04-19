# Module: Write API

**Purpose:** Public API for posting, canceling, and re-posting movements with JSON validation, batch processing, and atomic execution.

## Files

- `post.c` — `register_post()` implementation
- `unpost.c` — `register_unpost()` implementation
- `repost.c` — `register_repost()` implementation

## Responsibilities

### 1. `register_post(name, data)` — Post Movements

- Accepts a single JSONB object or a JSONB array of objects
- Validates JSON structure: all declared dimensions and resources must be present
- Missing resources default to 0
- Computes `dim_hash` per movement
- Inserts into the movements table via SPI with parameterized queries
- Returns the count of inserted movements
- Triggers fire automatically, updating totals and balance_cache

### 2. `register_unpost(name, recorder)` — Cancel Movements

- Deletes all movements matching the given recorder from the movements table
- AFTER DELETE triggers automatically reverse the effect on totals and balance_cache
- Returns the count of deleted movements

### 3. `register_repost(name, recorder, new_data)` — Replace Movements

- Atomic operation: delete old movements + insert new movements in a single transaction
- Optimized path: if only resource values changed (same dimensions), computes and applies the net delta
- Fallback path: if dimensions changed, performs full unpost + post
- Triggers handle all derived table updates

## Security

- All SQL execution uses SPI with parameterized queries — no string concatenation
- JSONB input is validated against the register's declared schema before execution
- Register names are validated against the internal registry

## SQL Sources

- [sql/05_write_api.sql](../../sql/05_write_api.sql) — `register_post`, `register_unpost`, `register_repost` function definitions

## Related Tests

- [test/sql/03_register_post.sql](../../test/sql/03_register_post.sql) — Single and batch posting, validation
- [test/sql/04_register_unpost.sql](../../test/sql/04_register_unpost.sql) — Document cancellation
- [test/sql/05_register_repost.sql](../../test/sql/05_register_repost.sql) — Atomic movement replacement
- [test/sql/12_direct_insert.sql](../../test/sql/12_direct_insert.sql) — Trigger-based updates via raw INSERT
- [test/sql/17_recorder_pattern.sql](../../test/sql/17_recorder_pattern.sql) — Document-based post/unpost/repost lifecycle
