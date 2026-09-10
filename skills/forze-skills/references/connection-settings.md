# Connection settings

Every integration package ships a `<Backend>Settings` model holding the endpoint, the
credentials and the client knobs a deployment sets. It is the thing an application would
otherwise declare for itself, once per backend, and get subtly wrong — percent-encoding a
password, bracketing an IPv6 host, choosing `rediss://` over `redis://`.

## Mount them on your own settings root

They are plain pydantic `BaseModel`s. Forze ships no `BaseSettings` root, because the
environment prefix, the delimiter and the extra-key policy are deployment decisions:

```python
from forze_postgres import PostgresSettings
from forze_redis import RedisSettings
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    postgres: PostgresSettings = PostgresSettings()
    redis: RedisSettings = RedisSettings()
```

`POSTGRES__HOST`, `REDIS__PORT` and the rest then populate them.

## Feed the lifecycle step

Each model exposes exactly what its wiring takes — the connection string under the name
that backend uses, and the backend's own config object:

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import postgres_lifecycle_step

settings = Settings()

lifecycle = LifecyclePlan.from_steps(
    postgres_lifecycle_step(dsn=settings.postgres.dsn, config=settings.postgres.config),
)
```

| It gives you | Where it goes |
|---|---|
| `.dsn` / `.uri` / `.url` / `.address` / `.servers` | the lifecycle step's connection argument |
| `.config` | the same step's `config=`, as the backend's own config object |
| `.require_host()` / `.require_endpoint()` / `.require_project_id()` | wherever the wiring wants a plain `str` |

## Four rules that hold across all of them

- **A missing endpoint is refused by name when it is read**, not when the model is built.
  `Settings()` still constructs against an empty environment, and a boot without
  `POSTGRES__HOST` fails saying so rather than quietly dialling localhost.
- **Unset knobs are dropped, never forwarded as `None`.** The defaults live in the
  backend's own config object; the settings model keeps no second copy of them.
- **Secrets are `SecretStr`,** and so is any assembled URL carrying a credential. The
  assembled value is a property rather than a field, so `model_dump()` works on a root
  that mounts a backend it never configured, and no DSN lands in a dump.
- **`ssl=True` means a verified connection** everywhere — `sslmode=verify-full`,
  `neo4j+s://`, `rediss://`, `https://`. A deployment wanting weaker TLS leaves the flag
  off and configures the backend's own environment.

## The process itself

`RuntimeSettings` is the same idea for the argument lists of `bootstrap_logging` and
`bootstrap_telemetry`:

```python
from forze.base.settings import RuntimeSettings

rt = RuntimeSettings(version=APP_VERSION, build_id=BUILD_ID, telemetry="otlp")

bootstrap_logging(level=rt.log_level, render_mode=rt.log_render)
bootstrap_telemetry(
    service_name="orders-api",
    service_version=rt.full_version,
    exporter=rt.telemetry,
)
```

It defaults `log_render` to `json`, unlike `bootstrap_logging` itself — a settings object
exists because something is being deployed.

`EndpointSettings` is the shared base under the URL-building models: the `host`/`port`
pair and the URL-authority grammar that applies to every scheme built from it. The scheme
and the query parameters stay in each package, because that is the part that differs.

## Which packages have one

All of them except `fastapi` and `mcp`, for the same reason both times: they are inbound,
and where they bind is the deployment's uvicorn or transport concern.

The ones that are barely a connection still have one — `DuckDbSettings` carries the
database path and the two resource limits that decide whether a query is slow or the
container is killed; `SocketIOSettings` carries the Redis backplane — the URL, the
channel, and whether this process publishes without subscribing (build the URL from a
`RedisSettings.dsn`, since integration packages never import each other); `GcpKmsSettings`
is an emulator endpoint and a timeout, because Google's credentials come from the ambient
environment.

## Anti-patterns

- **Building a DSN by f-string in application code** — the model already handles the
  credential encoding, the IPv6 brackets and the TLS scheme.
- **Mounting a settings model on a root that dumps to logs without checking** — the URL
  properties stay out of `model_dump()`, but a `SecretStr` you unwrap yourself does not.
- **Re-declaring a backend default on the settings model** — leave it unset and let the
  backend's own config object own it.

## Reference

- [Integrations → Connection settings](https://morzecrew.github.io/forze/latest/integrations/)
