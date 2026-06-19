# Remote ClickHouse Tutorial

Use this when you are one person running Moraine against an existing
ClickHouse database, such as a Docker container you manage yourself, a NAS, or
a personal server. In this setup the remote database is Moraine's default
backend: every indexed session goes there, `moraine db migrate` applies schema
migrations there, and agent MCP search reads from there.

This is different from team mirroring. If you want to keep a complete local
database and mirror only selected projects to another server, use
[Backends and Per-Project Routing](configuration.md#backends-and-per-project-routing)
instead.

## Before You Start

Moraine talks to ClickHouse over the HTTP or HTTPS interface. The machine
running Moraine needs:

- a reachable ClickHouse URL, such as `http://127.0.0.1:8123` or
  `https://clickhouse.example.net:8443`;
- a database for Moraine, normally `moraine`;
- credentials that can create and alter Moraine tables during migrations, then
  insert and query rows during normal operation.

Do not expose an unauthenticated ClickHouse HTTP port to the public internet.
For a personal server, prefer HTTPS behind a firewall, a VPN, or an SSH tunnel.

## Option 1: Docker On The Same Host

Run ClickHouse yourself and bind its HTTP port to localhost:

```bash
docker volume create moraine-clickhouse

docker run -d --name moraine-clickhouse \
  --restart unless-stopped \
  -p 127.0.0.1:8123:8123 \
  -v moraine-clickhouse:/var/lib/clickhouse \
  -e CLICKHOUSE_DB=moraine \
  -e CLICKHOUSE_USER=moraine \
  -e CLICKHOUSE_PASSWORD='change-me' \
  clickhouse/clickhouse-server:25.12.5.44
```

Check that the endpoint answers:

```bash
curl -u 'moraine:change-me' 'http://127.0.0.1:8123/?query=SELECT%201'
```

Then edit `~/.moraine/config.toml`:

```toml
[clickhouse]
url = "http://127.0.0.1:8123"
database = "moraine"
username = "moraine"
password = "change-me"
timeout_seconds = 30.0
async_insert = true
wait_for_async_insert = true
```

Start the Docker container before `moraine up`. When the configured endpoint is
already healthy, Moraine treats ClickHouse as unmanaged and only starts the
Moraine services around it.

## Option 2: Personal Server

On the server, create a database and a user for Moraine with enough privileges
to run migrations in that database. The exact provisioning commands depend on
how you operate ClickHouse, but the result should be:

- database: `moraine`;
- user: `moraine` or another dedicated service user;
- password or other HTTP(S)-compatible authentication;
- network access from your workstation to the ClickHouse HTTP(S) endpoint.

Then configure Moraine on your workstation:

```toml
[clickhouse]
url = "https://clickhouse.example.net:8443"
database = "moraine"
username = "moraine"
password = "replace-with-your-secret"
timeout_seconds = 30.0
async_insert = true
wait_for_async_insert = true
```

If you use an SSH tunnel instead of exposing HTTPS, point `url` at the local
side of the tunnel:

```bash
ssh -N -L 18123:127.0.0.1:8123 user@clickhouse-host
```

```toml
[clickhouse]
url = "http://127.0.0.1:18123"
database = "moraine"
username = "moraine"
password = "replace-with-your-secret"
```

Keep the tunnel running before starting Moraine.

## Initialize The Schema

Run a health check first:

```bash
moraine db doctor
```

Apply Moraine's schema migrations to the configured default database:

```bash
moraine db migrate
```

Run the doctor check again:

```bash
moraine db doctor
```

If migrations fail because the user is too restricted, either grant the Moraine
user migration privileges in the `moraine` database or run `moraine db migrate`
with a temporary admin config and switch back to the service config for normal
operation.

## Start Moraine

Start the stack normally:

```bash
moraine up
```

When the configured ClickHouse endpoint is already healthy, the `clickhouse`
row in the startup table should read `already serving (unmanaged)`. Moraine
then starts ingest, the monitor UI, and the shared MCP server against that
database.

Check status:

```bash
moraine status
```

The monitor still runs on the configured monitor address, normally:

```text
http://127.0.0.1:8080
```

## Troubleshooting

If `moraine up` fails for a non-local URL, Moraine did not start a local
ClickHouse fallback. Start or repair the configured ClickHouse endpoint, then
retry.

If `moraine up` starts a managed local ClickHouse when you expected Docker,
the configured local endpoint was not healthy before startup. Start the Docker
container or tunnel first, verify it with `moraine db doctor`, then run
`moraine up` again.

If `moraine db doctor` reports missing tables, run `moraine db migrate` against
the same config file used by `moraine up`.
