# Authentication and Authorization

## Basics

Two distinct concerns sit on top of every database connection:

- **Authentication** — *who* is the caller? Establishes identity, usually
  by having the caller supply a credential (a token, a password, a
  client certificate).
- **Authorization** — *may they do this?* Establishes whether an already
  authenticated caller is allowed to run a particular query, against a
  particular set of objects.

Quack runs these as two separate hooks: one when a client first
connects, and one before each query the client wants to issue.

## Defaults in Quack

Authentication and authorization of database queries are an endless
source of joy and complexity. We are unlikely to capture everyone's use
case in a first release, so we don't try. Instead, Quack ties into
DuckDB's philosophy of extensibility: ship sensible defaults, expose
them as overridable callbacks.

Out of the box:

- **Authentication is token-based.** When you call `rpc_start`, the
  server generates a random token and returns it in the `auth_token`
  column. Clients have to present this token (via a quack secret or the
  `rpc_default_token` setting) on every connection. The default
  authentication callback simply compares the supplied token against
  the server's `rpc_default_token` setting.
- **Authorization is permissive.** The default authorization callback
  returns `true` for every query — no further filtering happens.

Both callbacks can be replaced with user-supplied code, including plain
SQL macros — see below.

## The callback contract

Two settings hold the **name** of the function to call:

| Setting                       | Default                   | Called when                                 |
|-------------------------------|---------------------------|---------------------------------------------|
| `rpc_authentication_function` | `rpc_auth_token`          | A new client connects (`CONNECTION_REQUEST`). |
| `rpc_authorization_function`  | `rpc_dummy_authorization` | A client issues a query (`PREPARE_REQUEST`).  |

The server invokes them by literally running:

```sql
-- on every CONNECTION_REQUEST
SELECT <rpc_authentication_function>(<session_id>, <client_token>);

-- on every PREPARE_REQUEST
SELECT <rpc_authorization_function>(<connection_id>, <query>);
```

Both calls expect a `BOOLEAN` return: `true` admits the request,
anything else (including a query error) rejects it with
"Authentication failed" / "Authorization failed".

The arguments are always `VARCHAR`:

| Hook        | First arg              | Second arg                                     |
|-------------|------------------------|------------------------------------------------|
| Auth        | Server-generated session id (random 32-char). Becomes the `rpc_connection_id` for that client. | The auth string the client sent.               |
| Authz       | The `rpc_connection_id` of the calling client (i.e. the same id the auth hook saw as its first arg). | The full SQL text the client wants to execute. |

The first arg of the authz hook lets you correlate against state your
auth hook recorded — e.g. mapping a connection id to a user name.

The callbacks run in a **fresh, transient server-side `Connection`**.
That means they can read tables, call other UDFs, and reference
extensions, but each invocation starts a new session — they cannot
rely on session-local state.

Anything resolvable as a 2-argument `(VARCHAR, VARCHAR) → BOOLEAN`
function will work: built-in scalar functions, scalar UDFs registered
by another extension, or SQL macros.

## Overriding authentication

The cleanest way to plug in custom auth is a `MACRO` — no extension
required.

### Example: multi-token table

Authenticate against a small table of allowed tokens (e.g. one per
user):

```sql
CREATE TABLE rpc_tokens (auth_token VARCHAR, user_name VARCHAR);
INSERT INTO rpc_tokens VALUES
    ('alice-key-123', 'alice'),
    ('bob-key-456',   'bob');

CREATE MACRO check_token(sid, supplied_token) AS (
    EXISTS (SELECT 1 FROM rpc_tokens WHERE auth_token = supplied_token)
);

SET rpc_authentication_function = 'check_token';
```

Now any client whose token is in `rpc_tokens` is admitted; everyone
else is rejected. Adding/removing users is a regular `INSERT` /
`DELETE`.

### Example: dev mode (always allow)

Useful when iterating locally:

```sql
CREATE MACRO yolo_auth(sid, token) AS (true);
SET rpc_authentication_function = 'yolo_auth';
```

### Going beyond macros

DuckDB SQL macros are pure — they cannot have side effects, so they
can't write to a log table or call out to LDAP. For those cases use a
**scalar UDF** instead, registered via the C++ or Python extension API,
with the same `(VARCHAR, VARCHAR) → BOOLEAN` signature. Then point
`rpc_authentication_function` at its registered name. The dispatch is
identical.

## Overriding authorization

Authorization runs once per `PREPARE_REQUEST`, with the connection id
and the full SQL text. Common shapes:

### Example: read-only

```sql
CREATE MACRO read_only(sid, query) AS (
    regexp_matches(upper(trim(query)), '^(SELECT|FROM|WITH|EXPLAIN|DESCRIBE|SHOW)\b')
);

SET rpc_authorization_function = 'read_only';
```

### Example: per-user ACL

Pair this with a custom auth hook that records `(sid → user)` so
authorization can look up who is asking. Because macros can't write,
the recording side has to be a scalar UDF; the authz side can stay a
macro:

```sql
-- (populated by the auth UDF when a client connects)
CREATE TABLE rpc_sessions (sid VARCHAR PRIMARY KEY, user_name VARCHAR);

-- per-user query allowlist (your own data model)
CREATE TABLE rpc_user_acls  (user_name VARCHAR, query_kind VARCHAR);

CREATE MACRO acl_check(sid, query) AS (
    EXISTS (
        SELECT 1
        FROM rpc_sessions s
        JOIN rpc_user_acls a ON a.user_name = s.user_name
        WHERE s.sid = sid
          AND regexp_matches(upper(trim(query)), '^' || a.query_kind || '\b')
    )
);

SET rpc_authorization_function = 'acl_check';
```

### Caveat: scope of the authz hook

The authorization callback only fires on `PREPARE_REQUEST` — i.e. when
a client issues a SQL query that goes through the regular query path.
It does **not** fire on:

- `CATALOG_REQUEST` — `CREATE TABLE rpc.x …`, `DROP TABLE rpc.x`, and
  similar DDL go through a separate code path that does not consult
  the authz hook.
- `APPEND_REQUEST` — bulk inserts of `DataChunk`s into a remote table
  (issued by `INSERT INTO rpc.x …` after the planner resolves the
  target) skip authz; only the original `INSERT` SQL statement that
  produced the plan was checked.
- `FETCH_REQUEST` — once a query has been authorized and prepared, the
  client can pull all of its result chunks without any further
  authorization round-trip.

In other words: today, authorization gates *what queries can be
issued*, but it does not gate catalog mutations or row-level data
shipping initiated by an already-connected client. Keep this in mind
when designing a custom policy — for stronger isolation, lean on
authentication (e.g. don't hand out tokens to clients that should not
be able to write at all).

## Putting it all together

A self-contained example: a server that requires per-user tokens and
limits each user to read-only queries.

```sql
-- On the server session, before rpc_start:

CREATE TABLE rpc_tokens (auth_token VARCHAR, user_name VARCHAR);
INSERT INTO rpc_tokens VALUES ('analytics-team-token', 'analytics');

CREATE MACRO check_token(sid, supplied_token) AS (
    EXISTS (SELECT 1 FROM rpc_tokens WHERE auth_token = supplied_token)
);

CREATE MACRO read_only(sid, query) AS (
    regexp_matches(upper(trim(query)), '^(SELECT|FROM|WITH|EXPLAIN)\b')
);

SET rpc_authentication_function = 'check_token';
SET rpc_authorization_function  = 'read_only';

CALL rpc_start('quack:localhost');
```

A client with the right token now connects and can run `SELECT`s,
but `INSERT INTO rpc.t …` issued through the standard SQL path will
fail at authz time.
