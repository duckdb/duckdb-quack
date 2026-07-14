# DuckDB RPC Extension — Usage

The RPC extension turns a DuckDB instance into a server that other DuckDB
instances (clients) can connect to over HTTP(S). You can either `ATTACH` to to
the server or run one-off queries with a table function.

This guide walks through typical usage from both sides of the wire.

## Server-side usage

### Starting a server

A server is started from an existing DuckDB session. Everything that
session can see (in-memory tables, attached files, schemas) becomes
reachable over RPC.

```sql
-- HTTPS (default). Generates a self-signed cert on first use.
CALL rpc_generate_keys();
CALL rpc_start('quack:localhost');

-- Plain HTTP (no TLS)
CALL rpc_start('quack:localhost', disable_ssl => true);
```

`rpc_start` returns the listen URI, the HTTP(S) URL, and — when the
default authentication function is in use — an `auth_token` that clients
need to connect. This token can also be [set](#authentication--authorization).

### URI format

RPC endpoints use the `quack:` scheme, some examples:

| URI                       | Host        | Port (default 9494) |
|---------------------------|-------------|---------------------|
| `quack:localhost`         | `localhost` | `9494`              |
| `quack:myhost:9000`       | `myhost`    | `9000`              |
| `quack:127.0.0.1`         | `127.0.0.1` | `9494`              |
| `quack:[::1]:1234`        | `::1`       | `1234` (IPv6)       |
| `quack://localhost`       | `localhost` | `9494`              |

You can parse and validate a URI with the `rpc_uri_parser(uri, ssl)`
scalar function.

### TLS keys

`rpc_generate_keys()` writes `server.pem`, `private_key.pem`, and
`dh.pem` into DuckDB's default certificate directory. It is a no-op if
keys already exist — delete them to regenerate.

### Stopping a server

```sql
CALL rpc_stop('quack:localhost');
```

## Client-side usage

There are two ways to talk to an RPC server:

1. `rpc_call(uri, query)` — one-shot, stateless query.
2. `ATTACH 'quack:host' AS name` — attach the remote as a full catalog.

### Stateless queries with `rpc_call`

Run any SQL against a remote server without mounting it:

```sql
-- Default: HTTPS
FROM rpc_call('quack:localhost', 'SELECT 42');

-- Plain HTTP
FROM rpc_call('quack:localhost', 'SELECT 42', disable_ssl => true);
```

The query executes remotely and the result streams back. Errors from the
server (parse errors, missing tables, etc.) surface as DuckDB errors on
the client.

### Attaching a remote database

```sql
ATTACH 'quack:localhost' AS rpc;
-- or without TLS:
ATTACH 'quack:localhost' AS rpc (disable_ssl true);
-- with an explicit token and a client id (identifies the client across (re)connections):
ATTACH 'quack:localhost' AS rpc (token 'super_secret', client_id 'my_client');
```

Once attached, remote tables look local:

```sql
FROM rpc.fuu;                       -- scan remote table
FROM rpc.main.fuu WHERE col0 = 42;  -- filter + projection pushdown
CREATE TABLE rpc.t AS FROM range(10);  -- DDL on remote
INSERT INTO rpc.t VALUES (42);      -- remote writes
BEGIN; ... COMMIT;                  -- transactions are forwarded
DETACH rpc;
```

The attached catalog also exposes a `call` table macro for ad-hoc SQL
scoped to that attachment:

```sql
FROM rpc.call('SELECT 42');
```

### Pushdown

Scans through an attached catalog support both **projection pushdown**
and **filter pushdown** (constant comparisons, `IS NULL`, `IS NOT NULL`,
`IN`, and `AND`/`OR` combinations). Only the required columns are
transferred, and filters are evaluated server-side. Verify with
`EXPLAIN`:

```sql
EXPLAIN SELECT i FROM rpc.main.test_data WHERE i = 42;
```

### Authentication

When the server uses the default auth function (`rpc_auth_token`),
clients must have the server's token set in their session:

```sql
SET rpc_default_token = '<token-from-rpc_start>';
```

---

## Function reference

### Server management

| Function                                     | Description                                               |
|---------------------------------------------|-----------------------------------------------------------|
| `rpc_start(uri, disable_ssl := false)`      | Start a server on `uri`. Returns listen URI, URL, token.  |
| `rpc_stop(uri)`                             | Stop the server listening on `uri`.                       |
| `rpc_generate_keys()`                       | Generate self-signed TLS keys in DuckDB's cert directory. |

### Client queries

| Function                                         | Description                                                                 |
|-------------------------------------------------|-----------------------------------------------------------------------------|
| `rpc_call(uri, query, disable_ssl := false)`    | Run `query` on remote `uri`, stream result back.                            |
| `rpc_call_by_name(catalog, query)`              | Run `query` against an already-attached RPC catalog (used by `db.call()`). |

### Utility

| Function                      | Description                                                         |
|------------------------------|---------------------------------------------------------------------|
| `rpc_uri_parser(uri, ssl)`   | Parse an RPC URI into `{host, port, ipv6, ssl, url}`.               |
| `rpc_auth_token(sid, token)` | Default authentication callback; compares against `rpc_default_token`. |
| `rpc_dummy_authorization(sid, query)` | Default authorization callback; always allows.              |

### `ATTACH` options

| Option        | Type    | Default                            | Description                                                                                                                                              |
|---------------|---------|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `disable_ssl` | BOOLEAN | `false`                            | Use plain HTTP instead of HTTPS.                                                                                                                          |
| `token`       | VARCHAR | quack secret / `rpc_default_token` | Auth token sent to the server; overrides any matching quack secret.                                                                                      |
| `client_id`   | VARCHAR | *(none)*                           | Opaque client identifier. The server derives a stable per-client hash `md5(token + client_id)`, exposed as `client_id_hash` in `quack_active_connections()`. |

---

## Logging

Two log types are registered by the extension. Enable them to debug
connectivity or measure request timing.

### `RPC` log

Structured log of every RPC message (both client- and server-side):

```sql
CALL enable_logging('RPC');

FROM rpc_call('quack:localhost', 'SELECT 42');

SELECT * FROM duckdb_logs_parsed('RPC');
```

Fields on each entry:

| Field               | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| `message_type`      | Request type: `PREPARE_REQUEST`, `FETCH_REQUEST`, etc.             |
| `rpc_connection_id` | Server-issued connection id (stable across requests in one ATTACH).|
| `client_query_id`   | Monotonic id assigned by the client; correlates client/server logs.|
| `query`             | SQL payload for `PREPARE_REQUEST`s.                                |
| `server`            | HTTP(S) URL on client-side logs; NULL on server-side logs.         |
| `duration_ms`       | Round-trip time (client) or handling time (server).                |
| `response_type`     | Response type, or `ERROR`.                                          |
| `error`             | Error message if the request failed.                               |

To correlate a client request with its server-side handling, join on
`(rpc_connection_id, client_query_id)`.

### `HTTP` log

The underlying HTTP transport can be logged separately:

```sql
CALL enable_logging('HTTP');
FROM rpc_call('quack:localhost', 'SELECT 1');
SELECT request.type, request.url, response.status
FROM duckdb_logs_parsed('HTTP');
```

Requests are `POST`s to a `/rpc` endpoint.

### Persisting logs for querying

`duckdb_logs_parsed` reads from DuckDB's in-memory log buffer. For
non-trivial sessions you'll want to persist logs:

```sql
  CALL enable_logging(
      'RPC',
      storage => 'file',
      storage_config => {'path': '/tmp/duckdb-rpc-logs'}
  );
```

Use `CALL truncate_duckdb_logs();` to clear between runs and
`CALL disable_logging();` to turn logging off.

---

## Settings

All settings are regular DuckDB session/global options. Set with
`SET <name> = <value>` or `SET GLOBAL`.

### Authentication / authorization

| Setting                         | Type    | Default                   | Description                                                             |
|---------------------------------|---------|---------------------------|-------------------------------------------------------------------------|
| `rpc_authentication_function`   | VARCHAR | `rpc_auth_token`          | Name of a 2-arg scalar function `(sid, token) -> BOOLEAN` used by the server to authenticate clients. |
| `rpc_authorization_function`    | VARCHAR | `rpc_dummy_authorization` | Name of a 2-arg scalar function `(sid, query) -> BOOLEAN` used by the server to authorize each query. |
| `rpc_default_token`             | VARCHAR | *(unset)*                 | Shared secret. The server generates one on `rpc_start`; clients must set it before connecting. |

You can plug in your own auth by creating any scalar function with the
expected signature and pointing the setting at it.

### FETCH batching (server-side)

The server batches multiple `DataChunk`s into each `FETCH` response to
reduce per-chunk overhead. Tune with:

| Setting                     | Type    | Default      | Description                                       |
|----------------------------|---------|--------------|---------------------------------------------------|
| `quack_fetch_batch_chunks`  | UBIGINT | `12`         | Max `DataChunk`s shipped per FETCH response.      |
| `quack_fetch_batch_bytes`   | UBIGINT | `4194304`    | Max estimated payload bytes per FETCH response (4 MiB). |

A FETCH returns as soon as either limit is hit.
