---
title: Overview
icon: lucide/plug
summary: One optional package per backend, each behind a stable contract
---

Every integration is an optional package — `forze[postgres]`, `forze[redis]`,
and so on — that implements Forze contracts for one backend. They share a common
shape: a client and ports in `kernel/`, concrete `adapters/`, and `execution/`
deps + lifecycle modules you wire exactly as shown in
[Wiring](../writing-operation/wiring.md). Your handlers never import them; they resolve
ports from the context.

Per-backend pages cover the specifics — configuration, schema expectations, and
caveats. The authoritative, always-current set is the `[project.optional-dependencies]`
extras in `pyproject.toml`; each `forze[<name>]` maps to the `<name>` extra.

## Available integrations

| Area | Extras |
|------|--------|
| **Data** | `postgres` · `mongo` · `firestore` · `neo4j` · `redis` · `s3` · `gcs` · `meilisearch` · `bigquery` · `clickhouse` · `duckdb` |
| **Messaging** | `kafka` · `rabbitmq` · `sqs` |
| **Workflows** | `temporal` · `inngest` |
| **Inbound** | `fastapi` · `socketio` · `mcp` |
| **Identity** | `authn` · `oidc` |
| **Secrets & keys** | `vault` · `kms-aws` · `kms-gcp` · `kms-yc` |
| **Outbound** | `http` · `inference-http` · `inference-sagemaker` |

Each row maps to a `forze[<extra>]` package; their precise contract coverage is
on each integration's page. Two key backends need no extra at all: the
[self-hosted local KMS](kms.md) and the in-memory mock.

The remaining extras are not backend integrations: `dst` and `cli` are tooling
([deterministic simulation testing](../dst/overview.md) and the `forze`
command-line tool), and `zstd` adds the zstd codec for
[portable archives](../running-in-prod/portability.md).

Install one or several at once — `uv add 'forze[fastapi,postgres,redis]'`.

## Connection settings

Every integration ships a `<Backend>Settings` model holding the endpoint, the credentials
and the client knobs a deployment sets — the thing an application would otherwise declare
for itself, once per backend, and get subtly wrong. They are plain pydantic `BaseModel`s,
so you mount them on your own settings root and the environment fills them in:

```python
from forze_postgres import PostgresSettings
from forze_rabbitmq import RabbitMQSettings
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    postgres: PostgresSettings = PostgresSettings()
    rabbitmq: RabbitMQSettings = RabbitMQSettings()
```

`POSTGRES__HOST`, `RABBITMQ__VHOST` and the rest then populate them. The prefix, the
delimiter and the extra-key policy stay yours — Forze ships no `BaseSettings` root,
because those are deployment decisions.

Each model exposes what its wiring takes:

| It gives you | Where it goes |
|---|---|
| `.dsn` / `.uri` / `.url` / `.address` / `.servers` | the lifecycle step's connection argument |
| `.config` | the same step's `config=`, as the backend's own config object |
| `.require_host()` / `.require_endpoint()` / `.require_project_id()` | wherever the wiring wants a plain `str` |

```python
lifecycle = LifecyclePlan.from_modules(
    postgres_lifecycle_step(dsn=settings.postgres.dsn, config=settings.postgres.config),
)
```

Three rules hold across all of them:

- **A missing endpoint is refused by name**, when it is read rather than when the model is
  built — so `Settings()` still constructs with an empty environment, and a boot without
  `POSTGRES__HOST` fails saying so instead of dialling localhost.
- **Unset knobs are dropped, not forwarded as `None`.** The defaults live in the backend's
  own config object, never in a second copy on the settings model.
- **Secrets are `SecretStr`** and the assembled URL is one too wherever it carries a
  credential, so neither reaches a log by accident. The assembled value is a plain
  property rather than a serialized field: `model_dump()` on a settings root works even
  where a mounted backend was never configured, and no DSN lands in a dump.
- **`ssl=True` verifies the server certificate** — `sslmode=verify-full`, `neo4j+s://`,
  `rediss://`, `https://`. A deployment that wants weaker TLS leaves the flag off and
  configures the backend's own environment variables.

The URL-building models share `forze.base.settings.EndpointSettings` for the parts that are
URL grammar rather than backend knowledge — bracketing a bare IPv6 host, joining the port.
The scheme and the query parameters stay in each package, because that is the part that
actually differs.

`forze.base.settings.RuntimeSettings` is the same idea for the process itself: the argument
lists of `bootstrap_logging` and `bootstrap_telemetry`. See
[Grafana stack](../running-in-prod/grafana-stack.md).

Two packages ship no settings model, for the same reason both times: they are *inbound*,
so there is no connection to configure. `fastapi` and `mcp` serve requests; where they bind
is the deployment's uvicorn or transport concern, not the integration's.

Everything else has one, including the ones that are barely a connection at all: `duckdb`
is in-process, so `DuckDbSettings` carries the database path and the two resource limits
that decide whether a query is slow or the container is killed; `SocketIOSettings` carries
only the Redis backplane URL (build it from a `RedisSettings.dsn`, since integration
packages do not import each other); `GcpKmsSettings` is one emulator endpoint and one
timeout, because Google's credentials come from the ambient environment.
