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

## Limitations

- COPY is **not atomic** — mid-batch failures leave partial data (JetStream at-least-once semantics)
- Subject filtering is client-side (full stream is fetched then filtered)
- Each query creates a new connection (no connection pooling)
- Publish is serial per-message ack (no pipelining)
- `json_extract` and predicate pushdown not yet implemented

## License

MIT
