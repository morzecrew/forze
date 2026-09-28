---
title: Production posture
icon: lucide/shield-check
summary: Refuse to boot with development settings — declared once, checked at wiring and at startup
---

Every incident that starts with "it was running with the dev settings" is a check nobody ran. A
`ProductionPosture` is that check, declared once by the deployment: these settings must be set,
these must be HTTPS, these values are development-only. The runtime evaluates it before it
starts, and a violation refuses the boot — naming the **settings and the rules they break, never
their values**, because a boot failure ends up in logs, collectors and tickets.

**An unset environment is production.** You name the environments that are not — `dev`, `test`,
`staging` — and every other value, including none at all, gets every rule. The dangerous default
is the strict one.

## Declaring a posture

The framework owns no settings class, so the posture carries yours and names fields by path:

```python
from pydantic import BaseModel, SecretStr

from forze.application.execution import (
    DEFAULT_RULES,
    DevValue,
    LoopbackHost,
    ProductionPosture,
    RequireHttps,
    RequireSet,
    build_runtime,
)


class Db(BaseModel):
    dsn: str | None = None
    password: SecretStr | None = None


class Settings(BaseModel):
    env: str | None = None
    db: Db = Db()
    public_base_url: str | None = None
    cors_allow_origins: list[str] = []


settings = Settings()

posture = ProductionPosture(
    settings=settings,
    environment="env",
    non_production={"dev", "test"},
    rules=(
        *DEFAULT_RULES,
        RequireSet(fields=("db.password",)),
        RequireHttps(fields=("public_base_url", "cors_allow_origins")),
        LoopbackHost(fields=("db.dsn", "public_base_url")),
        DevValue(pattern="*", fields=("cors_allow_origins",)),
    ),
)

runtime = build_runtime(posture=posture)
```

| Rule | Refuses | Reads |
|------|---------|-------|
| `RequireSet(fields=…)` | a field that is `None`, blank, an empty secret or an empty collection | the named paths |
| `RequireHttps(fields=…)` | a set value (or any element of a collection) that is not an `https` URL | the named paths |
| `LoopbackHost(fields=…)` | a URL, DSN or `host:port` pointing at `localhost` or a loopback address | the named paths |
| `DevValue(pattern=…, fields=…)` | a value containing the pattern — `"*"` for wildcard origins | the named paths, or **every** string field when `fields` is omitted |

`DEFAULT_RULES` is the one rule that needs no path: a `_dev_only` marker anywhere in the
settings, secrets included, refuses. Every other rule points at your own fields, because only you
know which one is the DSN. Extend the default rather than replacing it.

A path that names nothing in your settings is refused in **every** environment, development
included: a posture referring to a renamed field is a rule nobody enforces, and a laptop is where
that should surface. A path through an unset parent (`db.password` when `db` is `None`) reads as
unset.

## Where it is checked

- **`check_wiring(..., posture=posture)`** puts its refusals in the report's `findings`, beside
  unresolved operations. A finding makes `ok` false and `raise_if_failed` raise, so the CI gate
  that runs the wiring check catches a posture violation before a deployment does.
- **`build_runtime(posture=posture)`** evaluates it again when the runtime is built, and refuses
  it with `configuration` (code `production_posture_refused`). An application that never runs the
  wiring check still cannot boot with development settings in production.

## Integrations that escalate their own warnings

Some configs already know a hazard and warn about it, because they cannot know on their own
whether they are running on a laptop. An HTTP service that sends a credential or declared-sensitive
data over plaintext `http://` is one: its config warns when constructed. Under a production
posture the same condition refuses the boot, named `http_service:<name>`.

The legitimate case — a service mesh terminating TLS in a sidecar, so the hop is plaintext and the
network is not — is an exemption the deployment writes down, with its reason:

```python
from forze.application.execution import Exempt

Exempt(target="http_service:billing", reason="the mesh terminates TLS in a sidecar")
```

An exemption needs a non-blank reason, so it reads as a reviewed line. It matches a rule's
settings path as well as an integration's target; a path that does not resolve cannot be exempted.

!!! warning "Adopting a posture can refuse a deployment that boots today"

    That is the point, and it is still a break. Run `check_wiring` with the posture in CI first,
    against the production settings, and fix what it names before the runtime starts refusing.

!!! note "What a green posture means"

    That the declared rules passed — not that the deployment is safe. The posture is a checklist
    you write; the shipped rule is a starting point, and it grades nothing it was not told about.
