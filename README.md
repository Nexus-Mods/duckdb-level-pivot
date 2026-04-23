# LevelPivot

A DuckDB storage extension that wraps [LevelDB](https://github.com/google/leveldb) with **pivot semantics** — structured, multi-column relational tables stored as key-value pairs in LevelDB.

## Installation

```sql
SET allow_unsigned_extensions = true;
INSTALL level_pivot FROM 'https://nexus-mods.github.io/duckdb-level-pivot/current_release';
LOAD level_pivot;
```

Requires DuckDB v1.4.4. The extension is not yet in the DuckDB community registry, so `allow_unsigned_extensions` is required. The `httpfs` extension is also needed for the remote install — most DuckDB builds autoload it, but if you get a "requires httpfs" error, run `INSTALL httpfs; LOAD httpfs;` first.

To update to a newer version:

```sql
FORCE INSTALL level_pivot FROM 'https://nexus-mods.github.io/duckdb-level-pivot/current_release';
```

## How It Works

LevelPivot maps relational rows to LevelDB keys using a **key pattern**. Each attribute value in a row becomes a separate LevelDB entry, and SELECTs reassemble rows by grouping keys that share the same identity prefix.

Given a table with pattern `users##{group}##{id}##{attr}` and columns `[group, id, name, email]`:

```sql
INSERT INTO testdb.users VALUES ('admins', 'u1', 'Alice', 'alice@ex.com');
```

This produces two LevelDB keys:

```
users##admins##u1##name  → Alice
users##admins##u1##email → alice@ex.com
```

A SELECT reconstructs the row by iterating keys with the shared prefix `users##admins##u1##` and pivoting the attribute names back into columns:

```
group   | id | name  | email
admins  | u1 | Alice | alice@ex.com
```

## Quick Start

```sql
-- Attach a LevelDB database (creates the directory if needed)
ATTACH 'my_data' AS db (TYPE level_pivot, CREATE_IF_MISSING true);

-- Create a pivot table
CALL level_pivot_create_table('db', 'users', 'users##{group}##{id}##{attr}',
  ['group', 'id', 'name', 'email']);

-- Insert, query, update, delete — standard SQL
INSERT INTO db.users VALUES ('admins', 'u1', 'Alice', 'alice@ex.com');
SELECT * FROM db.users WHERE "group" = 'admins';
UPDATE db.users SET email = 'new@ex.com' WHERE "group" = 'admins' AND id = 'u1';
DELETE FROM db.users WHERE "group" = 'admins' AND id = 'u1';

-- Clean up
CALL level_pivot_drop_table('db', 'users');
DETACH db;
```

## Table Modes

### Pivot Mode (default)

Pivot tables use a key pattern to decompose rows into LevelDB key-value pairs. The pattern has three kinds of segments:

- **Literals** — fixed text like `users##` that scopes keys
- **Captures** `{name}` — identity columns that form the row's primary key
- **`{attr}`** — placeholder replaced by each attribute column name

```sql
CALL level_pivot_create_table('db', 'metrics', 'metrics##{host}##{ts}##{attr}',
  ['host', 'ts', 'cpu_pct', 'mem_mb'],
  column_types := ['VARCHAR', 'BIGINT', 'DOUBLE', 'BIGINT']);
```

Identity columns (`host`, `ts`) uniquely identify a row. Attribute columns (`cpu_pct`, `mem_mb`) each get their own LevelDB entry per row.

### Raw Mode

Raw tables provide simple key-value access without pivot logic. They must have exactly two columns.

```sql
CALL level_pivot_create_table('db', 'kv', NULL,
  ['key', 'value'], table_mode := 'raw');

INSERT INTO db.kv VALUES ('hello', 'world');
SELECT * FROM db.kv;
```

## ATTACH Options

```sql
ATTACH 'path/to/leveldb' AS db (
  TYPE level_pivot,
  READ_ONLY false,            -- open read-only (default: false)
  CREATE_IF_MISSING true,     -- create LevelDB dir if absent (default: false)
  block_cache_size 8388608,   -- LevelDB block cache in bytes (default: 8MB)
  write_buffer_size 4194304   -- LevelDB write buffer in bytes (default: 4MB)
);
```

In read-only mode, SELECT works but INSERT, UPDATE, and DELETE return an error.

## Column Types

By default all columns are VARCHAR. Pass `column_types` to use typed columns:

```sql
CALL level_pivot_create_table('db', 'metrics', 'metrics##{host}##{ts}##{attr}',
  ['host', 'ts', 'cpu_pct', 'mem_mb'],
  column_types := ['VARCHAR', 'BIGINT', 'DOUBLE', 'BIGINT']);
```

Values are stored as strings in LevelDB and cast on read. Supported types include VARCHAR, BIGINT, INTEGER, DOUBLE, BOOLEAN, and anything DuckDB's `TransformStringToLogicalType` accepts. Typed columns enable correct numeric comparisons and aggregations:

```sql
-- Numeric comparison (not lexicographic)
SELECT * FROM db.metrics WHERE ts > 1500;

-- Aggregation works on typed columns
SELECT SUM(mem_mb), MAX(cpu_pct) FROM db.metrics;
```

Omitting `column_types` defaults everything to VARCHAR for backward compatibility.

## JSON-Encoded Values

LevelDB databases sometimes store values as JSON — e.g. `"Alice"` instead of `Alice`, or `[1,2,3]` instead of a bare string. The `JSON` prefix tells LevelPivot to unwrap JSON on reads and re-encode on writes:

```sql
CALL level_pivot_create_table('db', 'tbl', 'tbl##{id}##{attr}',
  ['id', 'name', 'score'],
  column_types := ['VARCHAR', 'JSON VARCHAR', 'JSON BIGINT']);
```

- `name` is stored as a JSON string in LevelDB (`"Alice"`) — reads as `Alice`
- `score` is stored as a JSON number (`42`) — reads as `42` (same as bare for numbers)
- `id` is a bare identity column — no JSON encoding

### Syntax

Prefix any type string with `JSON ` (case-insensitive):

```sql
column_types := ['VARCHAR', 'JSON VARCHAR', 'JSON BIGINT', 'JSON DOUBLE']
```

### Round-trip behavior

| DuckDB value | Column type   | LevelDB bytes    | Read back    |
|---|---|---|---|
| `Alice`      | JSON VARCHAR  | `"Alice"`        | `Alice`      |
| `Bob "B"`    | JSON VARCHAR  | `"Bob \"B\""`    | `Bob "B"`    |
| `42`         | JSON BIGINT   | `42`             | `42`         |
| NULL         | any           | (no key written) | NULL         |
| `Alice`      | VARCHAR       | `Alice`          | `Alice`      |

JSON `null` values in LevelDB are read as DuckDB NULL.

### Restrictions

Identity columns (captures in the key pattern) **cannot** be JSON — they are used to construct LevelDB keys and must be bare strings. Attempting to use `JSON` on an identity column produces an error:

```sql
-- This will error:
CALL level_pivot_create_table('db', 'bad', 'bad##{id}##{attr}',
  ['id', 'val'], column_types := ['JSON VARCHAR', 'VARCHAR']);
-- Error: Identity column 'id' cannot be JSON-encoded
```

Similarly, the key column of a raw table cannot be JSON.

## Filter Pushdown

Equality filters on consecutive identity columns (in pattern order) are converted to LevelDB prefix seeks:

```sql
-- Full prefix seek: seeks directly to users##admins##u1##
SELECT * FROM db.users WHERE "group" = 'admins' AND id = 'u1';

-- Partial prefix seek: seeks to users##admins##, scans within
SELECT * FROM db.users WHERE "group" = 'admins';

-- No prefix optimization (post-filter only — still works, just scans all keys)
SELECT * FROM db.users WHERE id = 'u3';
SELECT * FROM db.users WHERE name = 'Bob';
```

## NULL Handling

- **Identity columns** cannot be NULL (INSERT will error).
- **Attribute columns** can be NULL — no LevelDB key is stored for that attribute.
- Setting all attributes to NULL via UPDATE causes the row to vanish (no keys remain for that identity).

```sql
INSERT INTO db.users VALUES ('testers', 'u6', 'Frank', NULL);
-- Row exists with email = NULL

UPDATE db.users SET name = NULL WHERE "group" = 'testers' AND id = 'u6';
-- Row vanishes — both name and email are now NULL
```

## Data Persistence

LevelDB data persists to disk across DETACH/ATTACH cycles. However, **table definitions are transient** — after re-attaching, you must call `level_pivot_create_table` again to register the table schema. The underlying data is untouched.

```sql
ATTACH 'my_data' AS db (TYPE level_pivot, CREATE_IF_MISSING true);
CALL level_pivot_create_table('db', 'users', 'users##{group}##{id}##{attr}',
  ['group', 'id', 'name', 'email']);
INSERT INTO db.users VALUES ('admins', 'u1', 'Alice', 'alice@ex.com');
DETACH db;

-- Later...
ATTACH 'my_data' AS db (TYPE level_pivot);
CALL level_pivot_create_table('db', 'users', 'users##{group}##{id}##{attr}',
  ['group', 'id', 'name', 'email']);
SELECT * FROM db.users;  -- Alice is still here
```

## Dirty Table Tracking

LevelPivot tracks which tables have been modified within the current transaction. The `level_pivot_dirty_tables()` table function returns the set of tables that have received writes (INSERT, UPDATE, or DELETE) since the transaction began.

```sql
SELECT * FROM level_pivot_dirty_tables();
-- database_name | table_name | table_mode
-- testdb        | users      | pivot
-- testdb        | kv2        | raw
```

This is useful for change-detection workflows — for example, selectively syncing or reprocessing only the tables that changed. The dirty set resets when the transaction commits or rolls back.

Dirty tracking is key-aware: a raw-mode write only marks a pivot table as dirty if the written key actually matches that table's key pattern. For example, writing key `users##admins##u1##name` into a raw table will also mark the `users` pivot table dirty (since the key matches its pattern), but writing `something_else` will not.

## Additional Features

- **Multi-row INSERT**: `INSERT INTO db.t VALUES (...), (...), (...);`
- **INSERT INTO ... SELECT**: `INSERT INTO db.backup SELECT * FROM db.users WHERE "group" = 'admins';`
- **Column projection**: Only requested attribute keys are read from LevelDB.
- **DROP TABLE**: `CALL level_pivot_drop_table('db', 'table_name');`
- **SHOW TABLES**: `SELECT table_name FROM information_schema.tables WHERE table_catalog = 'db';`

## Building

### Dependencies

DuckDB extensions use vcpkg for dependency management. LevelPivot depends on `leveldb`:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

```shell
make
```

Produces:

- `./build/release/duckdb` — DuckDB shell with the extension loaded
- `./build/release/test/unittest` — test runner
- `./build/release/extension/level_pivot/level_pivot.duckdb_extension` — loadable extension binary

### Test

```shell
make test
```

Or run a specific test:

```shell
./build/release/test/unittest "test/sql/level_pivot.test"
```
