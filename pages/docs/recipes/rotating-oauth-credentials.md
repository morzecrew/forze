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
access the *store* makes is a point lookup on the primary key. The second index serves the
control-plane idleness scan (see [keeping idle grants alive](#keeping-idle-grants-alive)):

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
CREATE INDEX ON rotating_credentials (tenant_id, updated_at);
```

`tenant_id` is part of the key rather than a filter beside it — a table keyed on `ref`
alone hands one tenant another tenant's grant. An unbound tenant stores as the empty
string. `payload` holds both tokens and the provider metadata; `expires_at` is lifted out
as a column so an operator can find grants about to expire without reading secrets.

## Sealing at rest

`payload` is **sealed by default** — this is the one store whose every row is a replayable
long-lived credential, so a plaintext table turns a leaked logical backup or a read-only
replica into working third-party access for every tenant. It needs a keyring wired
(`CryptoDepsModule`; `forze_kms.local` is enough — no cloud KMS required), and wiring fails
closed if encryption is on without one.

The envelope's associated data binds each credential to its `(tenant, ref)`, which buys
something worth having quite apart from confidentiality: a row copied into another ref or
another tenant fails authentication instead of decrypting into the wrong grant. Only
`payload` is sealed, so `expires_at` stays queryable for operators.

Enabling it on an existing table needs no migration and no flag day — a plaintext row is
passed through on read and sealed on its next write, so the table converts as its grants
rotate. Going the other way is not symmetric: rows already sealed still need the key, and a
store wired without a cipher refuses them rather than returning garbage.

Storing credentials in the clear is possible and has to be said out loud:

```python
PostgresRotatingCredentialsConfig(
    relation=("public", "rotating_credentials"),
    exchanger=CrmTokenExchanger(http=...),
    encrypt=False,
    acknowledge_plaintext=True,   # required — `encrypt=False` alone is refused
)
```

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
from forze_postgres import PostgresDepsModule, PostgresRotatingCredentialsConfig

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
| `credential_exchange_timeout` | The token was presented and no answer came back | Re-authorize. The grant is marked unusable, because the token may already be spent |
| `credential_persist_lost` | The exchange succeeded and the commit did not | Re-authorize. The presented token is already burned, so no retry recovers it. Alert on this one |

The last two share a rule worth stating plainly: **once a refresh token has been presented,
it is never presented again.** A timeout is transient for the network but terminal for the
credential — the store cannot tell "they never saw it" from "they consumed it and the reply
was lost", and presenting a consumed token is reuse, which revokes the grant family. So the
store marks the grant unusable rather than leaving a row that still looks refreshable to the
next worker. The cost is a re-authorization that occasionally was not strictly necessary;
the alternative risks losing the whole family.

`credential_persist_lost` is the outcome worth wiring an alert to. It is rare, it is
logged at critical, and it always means one specific thing: a human has to re-consent.

## Keeping idle grants alive

On-demand refresh has a blind spot that no amount of on-demand logic can close: providers
expire a refresh token after a period of **non-use** — Google after six months, many
providers after 30–90 days — and the clock is reset by every exchange. A tenant with no
traffic never triggers a refresh, so its grant dies silently and permanently, and the next
real request finds a credential only a human can restore.

Two clocks, two mechanisms. `expires_at` is the *access token's* clock and drives
on-demand refresh. The refresh token's clock is idleness, and it is what
`CredentialSweeper` watches:

```python
from forze_kits.integrations.secrets import CredentialSweeper

sweeper = CredentialSweeper(
    # Set well inside the provider's documented inactivity window, with margin —
    # a missed sweep pass must not be fatal.
    refresh_if_idle_for=timedelta(days=30),
)
sweeper.register(durable_registry)

# Daily is almost always right: the idle window is weeks, the cadence just has to be
# dense enough that missing a couple of passes still lands well inside it.
await sweeper.ensure_cron(ctx, cron="0 4 * * *")
```

Each pass asks the control-plane scan (`RotatingCredentialsAdminPort.due_for_refresh`,
registered automatically beside the store) which grants sit unexchanged past the window,
oldest first, and enqueues **one durable refresh run per grant** — a dead provider costs
one failing run, never a stalled sweep. The runs call the same `refresh(observed=…)` as
live traffic with the version the scan saw, so a concurrent on-demand refresh converges to
one exchange instead of two; safety comes from the store, not from the sweeper.

Burnt grants show up in the sweep's result under `needs_reauthorization` rather than being
retried — "these N tenants need a human" is a queryable fact, not an alert someone may
have missed. Multi-tenant fleets drive the same sweep per tenant with
`sweeper.enqueue_tenants(...)`.

The idle window is per-provider **configuration**: an inactivity limit is a fact about
their product, and probing for it would spend a token. When in doubt, halve the documented
window.

## Trust but verify

Every store implementation runs the same conformance battery, and the Postgres store runs
it against a live database — including a failure injected at the write *and* one injected
at the commit, a two-connection race proving `FOR UPDATE` serializes across processes rather
than only across coroutines, the idleness-scan legs (an exchange resets the clock, the
scan is bounded, oldest first and tenant-scoped, burnt grants reported), and the at-rest legs: tokens unreadable on disk, a row lifted
across refs or tenants refused by the AAD, and a legacy plaintext row still readable. If you implement the port over another backend, run that
battery: the properties it checks are the reason the contract exists, and an
implementation that stores documents correctly while getting the ordering wrong is not one.

The runnable version of this recipe is `examples/recipes/rotating_credentials/`.
