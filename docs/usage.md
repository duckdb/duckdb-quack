# DuckDB RPC Extension — Usage

The RPC extension turns a DuckDB instance into a server that other DuckDB
instances (clients) can connect to over HTTP. You can either `ATTACH` to
the server or run one-off queries with a table function.

This guide walks through typical usage from both sides of the wire.

> **Transport note.** The server speaks plain HTTP only. The client
> can speak HTTP or HTTPS. HTTPS is intended for talking to a TLS reverse
> proxy in front of the server. For production use, put the Quack server
> behind a reverse proxy (nginx, Caddy, etc.) that terminates TLS — see
> [Securing Quack with a Reverse Proxy](./reverse-proxy.md).

## Server-side usage

### Starting a server

A server is started from an existing DuckDB session. Everything that
session can see (in-memory tables, attached files, schemas) becomes
reachable over RPC.

```sql
-- Start listening on localhost (the only host allowed by default).
CALL rpc_start('quack:localhost');
```

`rpc_start` returns the listen URI, the HTTP URL, and, when the
default authentication function is in use, an `auth_token` that clients
need to connect. This token can also be [set explicitly](#authentication)
before starting.

By default the server refuses to bind anything other than a local
hostname. To listen on an externally-reachable address, pass
`allow_other_hostname => true`:

```sql
CALL rpc_start('quack:0.0.0.0:1294', allow_other_hostname => true);
```

When you do this you should front the server with a TLS-terminating
reverse proxy — see [Securing Quack with a Reverse Proxy](./reverse-proxy.md).

### URI format

RPC endpoints use the `quack:` scheme, some examples:

| URI                       | Host        | Port (default 1294) |
|---------------------------|-------------|---------------------|
| `quack:localhost`         | `localhost` | `1294`              |
| `quack:myhost:9000`       | `myhost`    | `9000`              |
| `quack:127.0.0.1`         | `127.0.0.1` | `1294`              |
| `quack:[::1]:1234`        | `::1`       | `1234` (IPv6)       |
| `quack://localhost`       | `localhost` | `1294`              |

You can parse and validate a URI with the `rpc_uri_parser(uri, ssl)`
scalar function.

### Stopping a server

```sql
CALL rpc_stop('quack:localhost');
```

## Client-side usage

There are two ways to talk to an RPC server:

1. `rpc_call(uri, query)` — one-shot, stateless query.
2. `ATTACH 'quack:host' AS name` — attach the remote as a full catalog.

The client picks plain HTTP automatically for local URIs (`localhost`,
`127.0.0.1`, `::1`) and HTTPS otherwise. Override either way with
`disable_ssl`.

### Stateless queries with `rpc_call`

Run any SQL against a remote server without mounting it:

```sql
-- Local: HTTP by default.
FROM rpc_call('quack:localhost', 'SELECT 42');

-- Remote: HTTPS by default. Override if your proxy is plain HTTP.
FROM rpc_call('quack:remote.com', 'SELECT 42', disable_ssl => true);
```

The query executes remotely and the result streams back. Errors from the
server (parse errors, missing tables, etc.) surface as DuckDB errors on
the client.

### Attaching a remote database

```sql
-- Local: HTTP by default.
ATTACH 'quack:localhost' AS rpc;

-- Remote: HTTPS by default. Override on a plain-HTTP remote:
ATTACH 'quack:remote.com' AS rpc (disable_ssl true);
```

Once attached, remote tables look local:

```sql
FROM rpc.fuu;                       -- scan remote table
FROM rpc.main.fuu WHERE col0 = 42;  -- filter evaluated client-side (see Pushdown below)
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

> Currently pushdown is not being carried over to the server side.

### Authentication

Two ways to provide the auth token to a client.

**Quack secret (recommended).** Scope a secret to the server URI and the
client picks it up automatically:

```sql
CREATE SECRET (
    TYPE quack,
    TOKEN '<token-from-rpc_start>',
    SCOPE 'quack:localhost'
);

ATTACH 'quack:localhost' AS rpc (TYPE quack);
```

**Session setting (fallback).** If no matching secret is found, the
client falls back to `rpc_default_token`:

```sql
SET rpc_default_token = '<token-from-rpc_start>';
```

The server uses the value of `rpc_default_token` from its own session as
the expected token, which can either be set or provided when `rpc_start()`
is called.

Both the authentication and authorization checks the server runs are
overridable callbacks — see
[Authentication and Authorization](./auth.md) for the contract and
examples (read-only servers, per-user ACLs, etc.).

### Node identity (`whoami`)

Each Quack node exposes a `whoami()` table macro that surfaces basic
identity and runtime info — useful when proxying to a fleet of servers
or when correlating logs:

```sql
FROM rpc.call('FROM whoami()');
-- name | provider | hostname | region | uptime | ts_now | meta
```

Identity fields are populated either by setting `whoami_*` options
directly or by calling the `quack_identify` helper:

```sql
CALL quack_identify(
    name => 'analytics-1',
    provider => 'ec2',
    region => 'eu-west-1',
    meta => '{"role": "worker"}'
);
```

`meta` is merged with auto-computed `duckdb_version` and `platform`
keys; user-supplied keys win on conflict. `whoami_started_at` (an
ISO-8601 timestamp) overrides the uptime anchor; otherwise uptime is
measured from extension load.

---

## Function reference

### Server management

| Function                                                       | Description                                                                       |
|----------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `rpc_start(uri, allow_other_hostname := false)`                | Start a server on `uri`. Localhost-only by default. Returns listen URI, URL, token. |
| `rpc_stop(uri)`                                                | Stop the server listening on `uri`.                                               |
| `quack_identify(name, provider, hostname, region, meta)`       | Set this node's `whoami` identity fields. Any subset can be supplied.             |
| `whoami()`                                                     | Table macro returning identity + runtime info for the current node.               |

### Client queries

| Function                                         | Description                                                                          |
|--------------------------------------------------|--------------------------------------------------------------------------------------|
| `rpc_call(uri, query, disable_ssl := false)`     | Run `query` on remote `uri`, stream result back.                                     |
| `rpc_call_by_name(catalog, query)`               | Run `query` against an already-attached RPC catalog (used by `<catalog>.call()`).    |

### Utility

| Function                                | Description                                                            |
|-----------------------------------------|------------------------------------------------------------------------|
| `rpc_uri_parser(uri, ssl)`              | Parse an RPC URI into `{host, port, ipv6, ssl, url}`.                  |
| `rpc_auth_token(sid, token)`            | Default authentication callback; compares against `rpc_default_token`. |
| `rpc_dummy_authorization(sid, query)`   | Default authorization callback; always allows.                         |

### `ATTACH` options

| Option         | Type    | Default                  | Description                                                          |
|----------------|---------|--------------------------|----------------------------------------------------------------------|
| `disable_ssl`  | BOOLEAN | `true` for local, else `false` | Force the client transport. Local URIs default to plain HTTP.    |
| `type`         | VARCHAR | inferred                 | Pin the secret type used for token resolution (e.g. `quack`).        |


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
|---------------------|--------------------------------------------------------------------|
| `message_type`      | Request type: `PREPARE_REQUEST`, `FETCH_REQUEST`, etc.             |
| `rpc_connection_id` | Server-issued connection id (stable across requests in one ATTACH).|
| `client_query_id`   | Monotonic id assigned by the client; correlates client/server logs.|
| `query`             | SQL payload for `PREPARE_REQUEST`s.                                |
| `server`            | HTTP URL on client-side logs; NULL on server-side logs.            |
| `duration_ms`       | Round-trip time (client) or handling time (server).                |
| `response_type`     | Response type, or `ERROR`.                                         |
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

| Setting                         | Type    | Default                   | Description                                                                                            |
|---------------------------------|---------|---------------------------|--------------------------------------------------------------------------------------------------------|
| `rpc_authentication_function`   | VARCHAR | `rpc_auth_token`          | Name of a 2-arg scalar function `(sid, token) -> BOOLEAN` used by the server to authenticate clients.  |
| `rpc_authorization_function`    | VARCHAR | `rpc_dummy_authorization` | Name of a 2-arg scalar function `(sid, query) -> BOOLEAN` used by the server to authorize each query.  |
| `rpc_default_token`             | VARCHAR | *(unset)*                 | Shared secret. The server generates one on `rpc_start`; clients use it as fallback if no quack secret matches. |

You can plug in your own auth by creating any scalar function with the
expected signature and pointing the setting at it.

### FETCH batching (server-side)

The server batches multiple `DataChunk`s into each `FETCH` response to
reduce per-chunk overhead. Tune with:

| Setting                     | Type    | Default | Description                                  |
|-----------------------------|---------|---------|----------------------------------------------|
| `quack_fetch_batch_chunks`  | UBIGINT | `12`    | Max `DataChunk`s shipped per FETCH response. |

### Node identity

These settings back the `whoami()` macro. `quack_identify(...)` is sugar
that updates them.

| Setting               | Type    | Default                              | Description                                                |
|-----------------------|---------|--------------------------------------|------------------------------------------------------------|
| `whoami_name`         | VARCHAR | *(empty)*                            | Human-readable node name.                                  |
| `whoami_provider`     | VARCHAR | *(empty)*                            | Deployment provider (`ec2`, `docker`, `local`, ...).       |
| `whoami_hostname`     | VARCHAR | *(empty)*                            | Network hostname / public address.                         |
| `whoami_region`       | VARCHAR | *(empty)*                            | Deployment region.                                         |
| `whoami_started_at`   | VARCHAR | *(empty)*                            | Node start time (ISO-8601 timestamp). Anchors `uptime`.    |
| `whoami_meta`         | VARCHAR | `{}`                                 | Provider-specific metadata as JSON.                        |
| `quack_loaded_at_us`  | BIGINT  | epoch microseconds at extension load | Fallback uptime anchor when `whoami_started_at` is empty.  |
