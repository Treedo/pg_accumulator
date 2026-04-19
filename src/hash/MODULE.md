# Module: Hash

**Purpose:** Computes `dim_hash` — a 64-bit hash of the dimension value combination, used as the primary lookup key in totals and balance_cache tables.

## Files

- `hash.c` — Hash computation, per-register hash function generation

## Responsibilities

### 1. Hash Algorithm

- Default: xxhash64 (fast, low collision rate)
- Alternative: murmur3 (selectable via `pg_accumulator.hash_function` GUC)
- 64-bit output — collision probability below 1 in 10^18 for realistic dimension counts

### 2. Per-Register Hash Functions

Auto-generates `_hash_<register>(dim1, dim2, ...)` functions for each register. These functions are declared as `IMMUTABLE STRICT` and implemented in C for maximum performance.

### 3. Type Serialization

Each dimension type is serialized into a byte sequence before hashing:

| Type | Serialization |
|---|---|
| `int`, `bigint`, `smallint` | Native binary representation |
| `text`, `varchar` | UTF-8 bytes |
| `uuid` | 16 raw bytes |
| `date` | Julian day number |
| `boolean` | 1 byte (0 or 1) |

### 4. NULL Handling

NULL dimension values are serialized as a distinct sentinel byte, ensuring that `(warehouse=1, lot=NULL)` and `(warehouse=1, lot='')` produce different hashes.

## SQL Sources

- [sql/02_hash.sql](../../sql/02_hash.sql) — Hash function definitions

## Related Tests

- [test/sql/13_multiple_dimensions.sql](../../test/sql/13_multiple_dimensions.sql) — Multi-dimension hashing, NULL handling, collision avoidance
