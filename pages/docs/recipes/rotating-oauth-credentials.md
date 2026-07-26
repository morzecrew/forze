---
title: Survive single-use OAuth refresh tokens
icon: lucide/repeat
summary: Wire a store for credentials a provider rotates at you — persist-before-use, single-flight exchange, and a terminal burn notice
---

Some providers rotate the credential *at* you. Every refresh burns the token you
presented and returns a replacement, so by the time your code learns anything, the
rotation is already committed on their side. That makes two ordinary-looking failures
severe. A crash between the exchange and your write destroys the grant — the old token
is dead and the replacement is gone, recoverable only by a human re-consenting. And two
workers refreshing at once is not a wasted round trip: reuse detection treats a replayed
refresh token as an attack and may revoke the whole token family.

`RotatingCredentialStorePort` exists for exactly this shape. This recipe wires the
Postgres store, writes the exchanger, and shows the call pattern.

## The table

Provisioning is yours, and the store is deliberately narrow about what it needs — every
access is a point lookup, so the primary key is the only index:

```sql
CREATE TABLE rotating_credentials (
    tenant_id    text        NOT NULL,
    ref          text        NOT NULL,
    payload      jsonb       NOT NULL,
    expires_at   timestamptz,
    version      bigint      NOT NULL,
    burnt_reason text,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, ref)
);
```

`tenant_id` is part of the key rather than a filter beside it — a table keyed on `ref`
alone hands one tenant another tenant's grant. An unbound tenant stores as the empty
string. `payload` holds both tokens and the provider metadata; `expires_at` is lifted out
as a column so an operator can find grants about to expire without reading secrets.

Both tokens are stored in the clear. Protect this table the way you protect the
credentials it holds, and keep it out of logical backups that travel.

## The exchanger

The framework cannot supply this: it is a call to someone else's provider. Its one hard
obligation beyond being bounded is **classification**, because the store treats the two
cases oppositely.

```python
--8<-- "recipes/rotating_credentials/app.py:provider"
```

A permanent rejection — `invalid_grant` and its equivalents — is raised with
`code=INVALID_GRANT_CODE`, and the store records a terminal burn notice. Anything
transient (timeout, 5xx, connection reset) is raised as anything else, and the stored
credential is left exactly as it was. Reporting a transient failure as an invalid grant
destroys a working credential, so when the provider's answer is ambiguous, report it as
transient.

## Wiring

```python
from forze_postgres.execution.deps.configs import PostgresRotatingCredentialsConfig

module = PostgresDepsModule(
    rotating_credentials=PostgresRotatingCredentialsConfig(
        relation=("public", "rotating_credentials"),
        exchanger=CrmTokenExchanger(http=...),
        exchange_timeout=timedelta(seconds=15),
    ),
)
```

`exchange_timeout` is doing more than it looks. The store holds the credential's row lock
across the provider call, and derives the transaction's own
`idle_in_transaction_session_timeout` and `lock_timeout` from this bound. Without the
first of those, a server-side idle reaper could kill the transaction *between* a
successful exchange and its commit — manufacturing precisely the lockout the plane
exists to prevent. There is no unbounded setting.

## Using it

```python
--8<-- "recipes/rotating_credentials/app.py:call"
```

The `observed` version is the entire single-flight mechanism. Under the per-credential
lock the store re-reads first: if the stored version has moved past yours, someone
already exchanged and you get *their* credential rather than a second exchange. The
replacement is committed before `refresh` returns, so nothing observes a credential that
is not durable.

The refresh token never appears in what you hold — `get` returns the access token only,
and the store hands the refresh token straight to the exchanger. You cannot replay a
rotated token because you never have one.

## Getting the first grant, and getting back from a burn

```python
--8<-- "recipes/rotating_credentials/app.py:authorize"
```

`put` is unconditional by design. A human has just proven possession of a new grant, so
there is no earlier version worth defending — and refusing the write would leave a burnt
credential permanently unrecoverable.

## What the errors mean

| Code | Meaning | What to do |
| --- | --- | --- |
| `credential_burnt` | The provider has permanently rejected this grant | Re-authorize; retrying cannot help |
| `credential_exchange_timeout` | The store abandoned the exchange at its bound | Retry — the stored credential is untouched |
| `credential_persist_lost` | The exchange succeeded and the commit did not | Re-authorize. The presented token is already burned, so no retry recovers it. Alert on this one |

`credential_persist_lost` is the outcome worth wiring an alert to. It is rare, it is
logged at critical, and it always means one specific thing: a human has to re-consent.

## Trust but verify

Every store implementation runs the same conformance battery, and the Postgres store runs
it against a live database — including a failure injected at the write *and* one injected
at the commit, and a two-connection race proving `FOR UPDATE` serializes across processes,
not just across coroutines. If you implement the port over another backend, run that
battery: the properties it checks are the reason the contract exists, and an
implementation that stores documents correctly while getting the ordering wrong is not one.

The runnable version of this recipe is `examples/recipes/rotating_credentials/`.
