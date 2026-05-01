# Securing Quack with a Reverse Proxy

## Why

While Quack aims to "just work", attaching a database RPC directly to
the public internet is a security nightmare — and one that has been
abused before in similar systems. The extension's defaults reflect this
posture:

- The server generates a **random auth token** at startup, which the
  client has to supply on every connection.
- The server binds to `localhost` only; non-local hostnames require an
  explicit `allow_other_hostname => true`.
- The server does **not** speak SSL itself. Bringing TLS into the
  process just for localhost communication adds dependencies for no
  real benefit.

For any deployment beyond local-only, **do not expose Quack directly to
the internet**. The recommended pattern is the same as for any other
HTTP-based database / application server: put a battle-tested HTTP
reverse proxy in front of it, and let the proxy terminate TLS.

The Quack client cooperates with this: for non-local URIs it assumes
HTTPS by default, so a properly fronted server "just works" from the
client side too.

The two production setups below have been tested. They are
interchangeable — pick whichever fits your operational stack.

## Nginx + Let's Encrypt

The most common choice. A minimal site for a Quack server listening on
the loopback interface:

```nginx
# /etc/nginx/sites-enabled/quack.example.com
server {
    listen 443 ssl http2;
    server_name quack.example.com;

    ssl_certificate     /etc/letsencrypt/live/quack.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/quack.example.com/privkey.pem;

    # Quack RPC bodies can be large: PREPARE carries SQL, APPEND carries
    # inserted DataChunks. The 1 MiB nginx default will fail mid-INSERT.
    client_max_body_size 256M;

    # Long-running queries can sit on the wire between FETCHes for
    # minutes. Raise the timeouts above nginx's default 60s.
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;

    location / {
        proxy_pass http://127.0.0.1:1294;

        # Keep-alive against upstream — Quack relies on persistent
        # connections to keep server-side `rpc_connection_id` state alive.
        proxy_http_version 1.1;
        proxy_set_header Connection "";

        # Quack streams results via repeated FETCH responses; buffering
        # through nginx defeats the streaming and inflates memory.
        proxy_buffering off;
    }
}
```

On the Quack side, start the server bound to localhost (the default):

```sql
CALL rpc_start('quack:localhost');
```

Issue the certificate with `certbot --nginx -d quack.example.com`.
Clients connect over HTTPS automatically:

```sql
ATTACH 'quack:quack.example.com' AS rpc;   -- HTTPS auto-selected
```

## Caddy

[Caddy](https://caddyserver.com/) auto-provisions certificates from
Let's Encrypt and needs almost no configuration. A complete
public-facing Quack proxy:

```caddy
# /etc/caddy/Caddyfile
quack.example.com {
    reverse_proxy 127.0.0.1:1294 {
        # Equivalent of nginx `proxy_buffering off`. Required so Quack's
        # streamed FETCH responses pass through immediately instead of
        # being buffered in Caddy.
        flush_interval -1
    }

    # Equivalent of nginx `client_max_body_size`. PREPARE/APPEND bodies
    # can be much larger than the default request body cap.
    request_body {
        max_size 256MB
    }
}
```

Caddy handles certificate issuance and renewal automatically — no
`certbot` step. The Quack server-side commands and client-side
`ATTACH` are identical to the nginx setup above.

## Local test setup

You can exercise the full HTTPS path on your own machine using Caddy.
Caddy issues itself a certificate for `localhost` from a local CA and
installs the CA root into your system trust store, so DuckDB's HTTPS
client trusts the cert without any extra config.

### 1. Run Caddy

Save this `Caddyfile`:

```caddy
localhost:8443 {
    reverse_proxy 127.0.0.1:1294 {
        flush_interval -1
    }

    request_body {
        max_size 256MB
    }
}
```

Then start Caddy:

```bash
brew install caddy            # macOS — equivalents exist on Linux
caddy run --config Caddyfile
```

The first run will prompt for elevation to install Caddy's local CA
into your system trust store. After that, certs issued by Caddy for
`localhost` are trusted system-wide.

### 2. Start Quack and connect through the proxy

In one DuckDB session, start the server (this prints an auth token):

```sql
CALL rpc_start('quack:localhost');
```

In another session, connect through Caddy on `:8443`. Local URIs
default to plain HTTP, so you have to **force SSL on** explicitly:

```sql
SET rpc_default_token = '<auth_token-from-rpc_start>';

ATTACH 'quack:localhost:8443' AS rpc (disable_ssl false);

FROM rpc.call('SELECT 42');
-- 42
```

If the round-trip succeeds, your traffic just went out as TLS to Caddy,
got terminated, and was forwarded as plain HTTP to Quack on `:1294`.
