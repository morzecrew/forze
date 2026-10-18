---
title: Authentication
icon: lucide/key-round
summary: First-party authentication — passwords, tokens, and API keys
---

`forze[authn]` is the built-in authentication stack — the `forze_identity.authn`
plane. It implements the verify-then-resolve seam from [Identity &
access](../identity-tenancy-enc/identity.md): password (Argon2), bearer-token (Forze JWT),
and API-key verifiers, paired with resolvers that map a `VerifiedAssertion` to a
`UUID` principal.

## Install

```bash
uv add 'forze[authn]'
```

It's document-backed — the account, session, and identity-mapping stores are
ordinary document specs you wire to a database.

## Wire it

`AuthnDepsModule` registers a first-party stack per route. `authn` enables the
credential families a route accepts; `kernel` carries the signing/secret config:

```python
from forze.application.execution import DepsRegistry
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig

deps = DepsRegistry.from_modules(
    AuthnDepsModule(
        kernel=AuthnKernelConfig(...),
        authn={"api": frozenset({"token", "api_key"})},
    ),
)
```

A route's [`AuthnSpec`](../identity-tenancy-enc/identity.md) selects verifiers and a resolver
by profile name; override the defaults via `token_verifiers`, `resolvers`, etc.

By default every wired route checks the authenticating principal against an
active `policy_principal` document — the posture for deployments running the
authz plane. A token-only service with no authz plane opts out explicitly:

```python
AuthnDepsModule(..., eligibility="allow_all")
```

That is a declared decision, never a silent fallback (an unknown value is
refused at wiring): with it, deactivating a principal no longer blocks token
issuance, so revocation rests entirely on credential lifecycle — session revoke
and API-key revoke.

## What it provides

| Capability | Default implementation | Dep key |
|------------|------------------------|---------|
| Orchestrator | `AuthnOrchestrator` | `AuthnDepKey` |
| Token verifier | `ForzeJwtTokenVerifier` | `TokenVerifierDepKey` |
| Password verifier | `Argon2PasswordVerifier` | `PasswordVerifierDepKey` |
| API-key verifier | `HmacApiKeyVerifier` | `ApiKeyVerifierDepKey` |
| Principal resolver | `JwtNativeUuidResolver` | `PrincipalResolverDepKey` |

## Check sessions and API keys from Redis

By default every token verification reads its session row, and every API-key verification
its key row. With `revocation_states` on the kernel, a token carries its session's family
(`fam`) and the principal's epoch (`pep`), and the verifier checks them against states cached
in Redis, reading Postgres only for what Redis misses. With `key_mac` too, API keys are checked
the same way, from a state cached under the key's digest. The eligibility read stays for both.

```python
from datetime import timedelta

from forze.application.contracts.authn import RevocationStatesDepKey
from forze.application.contracts.resilience import PortPolicy
from forze_identity.authn import AuthnKernelConfig, RevocationStates, StateMacKeys
from forze_postgres import PostgresPrincipalEpochConfig
from forze_redis import RedisRevocationStatesConfig

kernel = AuthnKernelConfig(
    access_token_secret=secret,
    refresh_token_pepper=pepper,
    api_key_pepper=api_key_pepper,
    revocation_states=RevocationStates(
        route="authn-revocations",
        ttl=timedelta(minutes=5),
        key_mac=StateMacKeys(active=mac_key),  # optional: API keys too
    ),
)
states = {"authn-revocations": RedisRevocationStatesConfig(namespace="authn-revocations")}
epochs = PostgresPrincipalEpochConfig(relation=("public", "authn_principal_epochs"))
breaker = PortPolicy(
    key=RevocationStatesDepKey,
    policy="authn-revocations",
    methods=("read", "read_api_key"),
)
```

Wire `states` as `RedisDepsModule(revocation_states=...)`, `epochs` as
`PostgresDepsModule(principal_epochs=...)`, on the database that holds the sessions, and
`breaker` in `ResilienceDepsModule(port_policies=...)` under a policy with a circuit
breaker, so a Redis outage stops costing every verification its `timeout` (50 ms by
default). Bind it to the reads only: an open breaker on the writes would fail every attempt
of a revocation's write-through at once. The states route is resolved by `RevocationStatesSpec`
under `RevocationStatesDepKey`, and the epochs under `PrincipalEpochDepKey`. Create the epochs
table yourself and never delete its rows:

```sql
CREATE TABLE authn_principal_epochs (
    principal_id uuid   PRIMARY KEY,
    epoch        bigint NOT NULL
);
```

- **Transactions are required.** With the fast path on, issuing and revoking sessions refuse
  to run outside a transaction (`authn_revocation_requires_transaction`), so keep `tx_route`
  set on both registries. Issue and logout serialize on the epoch's row on Postgres only, with
  the revocation at read committed (the default); a revocation at repeatable read or
  serializable is refused (`authn_epoch_isolation`). Revoking an API key needs no
  transaction: its state is written once the key's row has committed, whichever way it runs.
- **`session_binding`.** `"session"` (the default) refuses a token once a refresh replaced its
  session, as without the fast path; `"family"` admits it until its own `exp` while the
  family is live. A logout, a family revocation and refresh-token reuse refuse it either way.
- **Minting re-reads the credential.** A tenant switch or an API-key issue in a request that
  authenticated with an access token re-reads that token's session and epoch from Postgres
  first, and one that authenticated with an API key (with `key_mac` set) re-reads the key's
  row (`api_key_revoked`) whoever the issue is for, under locks inside a transaction, so a
  credential admitted inside the window mints nothing that outlives it; above read committed a
  revocation committed since the transaction's snapshot fails it with a serialization error.
  A socket's commands run apart from the handshake that verified its token, and are not
  re-read.
- **Redis down.** A Redis error or timeout makes the same checks from Postgres, so an outage
  never admits a credential the fast path would refuse. Tokens issued before the fast path
  carry no `fam` or `pep`, and keep the session-row check until they expire.

### API keys

A key's cached state is the only proof the key exists: a hit reads no row, and the
verifier takes the principal, the delegation actor and the expiry from the state. So
everything able to write the states' Redis namespace is kept from minting a key:

- **Each live state carries a MAC** under `key_mac`, over the states route, the state's full
  Redis key (its namespace included), the key's digest and every field. A state forged
  without the key, copied onto another key's digest, or copied in from another namespace
  fails it and is read from Postgres. `revoked` carries none: forging it can refuse a key,
  never admit one.
- **Each live state carries a not-after**, the earlier of the key's own expiry and `ttl`
  after the lookup read Redis's clock. It is checked against Redis's clock from the same read,
  so a live state replayed over `revoked`, with no expiry of its own, stops verifying no later
  than any other state would. A state the application's clock puts more than `ttl` past its
  not-after is a miss too, so a Redis server that lies about its clock cannot stretch it
  further. The key's expiry is still checked on the application's clock.
- **A state that fails either check stays a miss** until it expires or is deleted: a lookup
  never overwrites a state, so a tampered key costs one row read per verification, never an
  admission. Keys nobody issued are never cached.

**Give every deployment its own `key_mac`.** The MAC binds a state to its route and
namespace, so deployments that share one Redis are kept apart; but two deployments with the
same namespace name and the same `key_mac`, a staging copy of production or customers served
from one secrets bundle, accept each other's states, and a state copied from one admits its key
in the other without a row read. Never reuse another secret as `key_mac` either: the kernel
refuses one equal to any of its peppers or its token secret (`authn_state_mac_key_reused`), and
each MAC key is used only through a key derived from it.

`StateMacKeys` takes keys of at least 32 bytes. To rotate one, make the new key `active`
and move the old one to `previous`, on every process that verifies or revokes; after `ttl`,
when every state signed under the old key has expired, drop it. A state signed under a key
no longer configured is read from Postgres, so dropping it early costs reads, not
admissions. A key carries no tenant, cached or not, and a deactivated principal's keys are
still refused by the eligibility read.

### The window

A revocation writes its states to Redis once it commits: `revoked` for each family, session
or API key it ended, or the raised epoch for a logout, a password change or reset, or a
deactivation. A revoked credential stays admitted for at most `ttl` after that commit, on
Redis's clock, and only when one of these leaves a stale state in place:

- **The write-through never lands.**
  - Every one of `write_attempts` failed. It is logged at error level and counted;
    revoking an API key again writes its state again.
  - The process died between the commit and the write. Nothing logs it.
  - The process that revoked was not configured, for example during a rolling deploy, from
    another service, or without `key_mac` for an API key. Nothing logs it.
  - The credential was revoked around the authn adapters, through its document port. Nothing
    logs it.
- **Redis lost the revocation's key while an earlier lookup was still writing:** an eviction,
  a flush, a restart, or a failover that dropped an acknowledged write. Nothing logs these.

An access token is also bounded by its own `exp`; an API key may have no expiry, so its
window is `ttl` alone. Configure every process that verifies or revokes, and give the states
a Redis namespace nothing else flushes or evicts.

**Redis's clock.** Every expiry and every not-after is set on Redis's clock, from `TIME` in
the script that reads or writes the state or relative to it with `PX`, so the bound has no
skew term against the application's clock. It does lean on the Redis primary's clock: a
failover to a replica whose clock lags lengthens every state by the lag, and moving the
primary's clock back does the same. Keep its clock synchronized, and let only operators change
it. The application's clock caps an API-key state at `ttl` past its not-after, so with the
application two `ttl` or more ahead of Redis every key state is refused as soon as it is
written, and every key verification reads its row; each such refusal is counted as
`clock_skew`.

### Metrics

`instrument_revocation_states({"default": kernel.revocation_states})` exports five counters,
labelled by credential (`session` or `api_key`):

| Counter | Counts |
|---|---|
| `forze.authn.revocation_states.hits` | verifications answered from Redis alone, refusals included |
| `forze.authn.revocation_states.misses` | verifications that read Postgres for a state Redis lacked or could not trust |
| `forze.authn.revocation_states.fallbacks` | verifications whose Redis read failed, timed out or met an open breaker |
| `forze.authn.revocation_states.write_failures` | revocations whose write-through failed every attempt |
| `forze.authn.revocation_states.clock_skew` | API-key states refused because the application's clock is two `ttl` or more ahead of Redis's |

Alert on any `write_failures`: each is a revoked credential admitted for up to `ttl`. A rising
`fallbacks` rate is Redis being down for verifications, a steady `clock_skew` is a clock to
fix, and `misses` against `hits` is what the cache saves. The [metrics reference](../reference/metrics.md#revocation-states) lists them with
the rest.

## Notes

- **Document-backed:** wire the account / session / identity-mapping specs to a
  document store (Postgres, Mongo, …). `kernel` is required only when a route
  mints first-party tokens.
- **Transactions:** the kit's login, refresh, logout, password change and reset, and
  deactivation run their writes in a transaction on `build_authn_registry(..., tx_route=...)`,
  and the tenant switch on `build_tenancy_registry(..., tx_route=...)`; both default to
  `"default"`. Register a transaction manager on each route you use, for the database
  holding the session and principal documents, e.g. `PostgresDepsModule(..., tx={"default"})`. A deactivation then commits
  whole. On Postgres an issue also reads the principal with a row lock that lasts until its
  session commits, so a concurrent deactivation either revokes the new session or refuses
  the issue; Mongo's locking read takes no lock, so there the race remains.
  `tx_route=None` runs the writes without a transaction and gives up both guarantees.
  Refresh refuses to run inside a transaction the caller already opened: a reused refresh
  token's family is revoked in a transaction of its own, which an enclosing one would roll
  back.
- **Browser clients:** the generated login/refresh/logout routes can carry their
  tokens in `HttpOnly` cookies instead of response bodies — see
  [cookie mode](../reference/fastapi-routes.md#cookie-mode).
- Authorization is a sibling plane — `forze_identity.authz` (`AuthzDepsModule`).
- The conceptual model (verify → resolve, resolver flavors) is in
  [Identity & access](../identity-tenancy-enc/identity.md); this page is the wiring.
- For external IdPs, pair this with the [OIDC](oidc.md) verifier.
