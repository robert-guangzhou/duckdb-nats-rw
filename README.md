# nats_rw

A DuckDB extension for reading and writing NATS JetStream streams with SQL.

## Features

- **Read**: `nats_rw_scan()` table function — bounded snapshot scan of JetStream streams
- **Write**: `COPY … TO '<stream>' (FORMAT nats_rw)` — publish query results to JetStream
- **Auth**: `CREATE SECRET (TYPE nats_rw, …)` — user/password, token, nkey, or credentials file
- **Metadata**: `nats_rw_streams()` and `nats_rw_stream_info()` — inspect JetStream state

## Quick Start

```sql
INSTALL nats_rw FROM community;
LOAD nats_rw;

-- Write
COPY (SELECT * FROM metrics) TO 'telemetry'
  (FORMAT nats_rw, url 'nats://localhost:4222', payload_column 'value', create_stream 'true');

-- Read
SELECT * FROM nats_rw_scan('telemetry', url := 'nats://localhost:4222');

-- Metadata
SELECT * FROM nats_rw_stream_info('telemetry', url := 'nats://localhost:4222');
```

## Options

### nats_rw_scan(stream, …)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream` | VARCHAR | (required) | JetStream stream name |
| `url` | VARCHAR | `nats://127.0.0.1:4222` | NATS server URL |
| `secret` | VARCHAR | (scope match) | DuckDB secret name for auth |
| `subject` | VARCHAR | (all) | NATS wildcard subject filter |
| `start_seq` | UBIGINT | 1 | Start sequence (inclusive) |
| `end_seq` | UBIGINT | last_seq | End sequence (inclusive) |
| `start_time` | TIMESTAMPTZ | (none) | Time range start (inclusive) |
| `end_time` | TIMESTAMPTZ | (none) | Time range end (inclusive) |
| `batch_size` | UBIGINT | 32 | Concurrent fetch parallelism [1, 256] |
| `json_extract` | VARCHAR[] | (none) | JSON field paths to extract as extra columns (e.g. `['device_id', 'location.zone']`) |

### COPY … TO (FORMAT nats_rw)

| Option | Description |
|--------|-------------|
| `url` | NATS server URL |
| `secret` | DuckDB secret name for auth |
| `subject_prefix` | Subject for published messages (default: stream name) |
| `payload_column` | Column to use as raw payload (default: whole row as JSON) |
| `create_stream` | `'true'` to create stream if missing (won't modify existing) |

### CREATE SECRET (TYPE nats_rw)

```sql
CREATE SECRET my_nats (
    TYPE nats_rw,
    URL 'nats://server:4222',
    USER 'username', PASSWORD 'password',   -- or TOKEN, NKEY, CREDS
    SCOPE 'nats://server');
```

#### Authentication options (pick one)

| Option | Description |
|--------|-------------|
| `USER` + `PASSWORD` | Username and password authentication |
| `TOKEN` | Single-token authentication |
| `NKEY` | NKey seed authentication |
| `CREDS` | Path to credentials file (JWT + seed) |

#### Using the secret

Once created, the secret can be used in three ways (all verified by `test/sql/secret-usage.test`):

**Way 1 — Explicit reference** (most explicit):

```sql
SELECT * FROM nats_rw_scan('my_stream',
    url := 'nats://server:4222',
    secret := 'my_nats');
```

**Way 2 — Scope auto-match** (no `secret` parameter needed):

If the secret's `SCOPE` matches the URL prefix, it's found automatically:

```sql
-- Secret was created with SCOPE 'nats://server'
-- No secret := needed — scope matches the url
SELECT * FROM nats_rw_scan('my_stream', url := 'nats://server:4222');
```

**Way 3 — Secret provides the URL** (omit `url` entirely):

If the secret has a `URL` option, it's used when you don't pass an explicit `url`:

```sql
-- URL comes from the secret, no url := needed
SELECT * FROM nats_rw_scan('my_stream', secret := 'my_nats');
```

#### Priority rules

- Explicit `url` parameter > secret's `URL` > default `nats://127.0.0.1:4222`
- Explicit `secret := 'name'` > scope-based auto-match
- Sensitive fields (`PASSWORD`, `TOKEN`, `NKEY`, `CREDS`) are redacted as `***` in `duckdb_secrets()`

#### COPY with secrets

```sql
-- Explicit secret reference
COPY (SELECT * FROM metrics) TO 'telemetry'
  (FORMAT nats_rw, secret 'my_nats', payload_column 'value', create_stream 'true');

-- Or scope auto-match (no secret option needed)
COPY (SELECT * FROM metrics) TO 'telemetry'
  (FORMAT nats_rw, url 'nats://server:4222', payload_column 'value');
```

#### Metadata with secrets

```sql
SELECT * FROM nats_rw_streams('', secret := 'my_nats');
SELECT * FROM nats_rw_stream_info('telemetry', secret := 'my_nats');
```

## Performance

- **Connection pooling**: Connections are cached per `(url, auth)` — repeated queries to the same server reuse a single NATS connection
- **Predicate pushdown**: `WHERE seq BETWEEN 3 AND 5` automatically narrows the scan range (no full-stream fetch)
- `json_extract` fields are parsed in the extension core (serde_json), adding columns without a separate JSON extension

## Limitations

- COPY is **not atomic** — mid-batch failures leave partial data (JetStream at-least-once semantics)
- Subject filtering is client-side (seq/time predicates are pushed down automatically)
- Publish is serial per-message ack (no pipelining)


## License

MIT — see [LICENSE](LICENSE) for details. Copyright © 2018-2025 Stichting DuckDB Foundation.
