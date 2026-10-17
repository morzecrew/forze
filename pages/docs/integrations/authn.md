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

## Check sessions from Redis

By default every token verification reads its session row. With
`revocation_states` on the kernel, a token carries its session's family (`fam`) and the
principal's epoch (`pep`), and the verifier checks them against states cached in Redis,
reading Postgres only for what Redis misses. The eligibility read stays, unless the authz
kernel carries the same configuration ([below](#skip-the-eligibility-read)).

```python
from datetime import timedelta

from forze.application.contracts.authn import RevocationStatesDepKey
from forze.application.contracts.resilience import PortPolicy
from forze_identity.authn import AuthnKernelConfig, RevocationStates
from forze_postgres import PostgresPrincipalEpochConfig
from forze_redis import RedisRevocationStatesConfig

kernel = AuthnKernelConfig(
    access_token_secret=secret,
    refresh_token_pepper=pepper,
    revocation_states=RevocationStates(route="authn-revocations", ttl=timedelta(minutes=5)),
)
states = {"authn-revocations": RedisRevocationStatesConfig(namespace="authn-revocations")}
epochs = PostgresPrincipalEpochConfig(relation=("public", "authn_principal_epochs"))
breaker = PortPolicy(key=RevocationStatesDepKey, policy="authn-revocations", methods=("read",))
```

Wire `states` as `RedisDepsModule(revocation_states=...)`, `epochs` as
`PostgresDepsModule(principal_epochs=...)`, on the database that holds the sessions, and
`breaker` in `ResilienceDepsModule(port_policies=...)` under a policy with a circuit
breaker, so a Redis outage stops costing every verification its `timeout` (50 ms by
default). Bind it to `read` only: an open breaker on the writes would fail every attempt of a
revocation's write-through at once. The states route is resolved by `RevocationStatesSpec` under
`RevocationStatesDepKey`, and the epochs under `PrincipalEpochDepKey`. Create the epochs
table yourself and never delete its rows:

```sql
CREATE TABLE authn_principal_epochs (
    principal_id uuid   PRIMARY KEY,
    epoch        bigint NOT NULL
);
```

- **`ttl` is the window.** A revocation writes its states to Redis once it commits. If that
  write never lands (it fails every one of `write_attempts`, which is logged at error level;
  the process dies first; the revoking process was not configured; or the session was revoked
  around the authn adapters, through its document port), or Redis loses the key while an
  earlier lookup is still writing, a revoked token stays admitted for at most `ttl` after the
  revocation commits. Configure every process that verifies or revokes, and give
  the states a Redis namespace nothing else flushes. Expiries are instants on the Redis
  primary's clock: a failover to a replica whose clock lags lengthens every state by the lag.
- **Transactions are required.** With the fast path on, issuing and revoking refuse to run
  outside a transaction (`authn_revocation_requires_transaction`), so keep `tx_route` set on
  both registries. Issue and logout serialize on the epoch's row on Postgres only, with the
  revocation at read committed (the default); a revocation at repeatable read or serializable
  is refused (`authn_epoch_isolation`).
- **`session_binding`.** `"session"` (the default) refuses a token once a refresh replaced its
  session, as without the fast path; `"family"` admits it until its own `exp` while the
  family is live. A logout, a family revocation and refresh-token reuse refuse it either way.
- **Minting re-reads the token.** A tenant switch or an API-key issue in a request that
  authenticated with an access token re-reads that token's session and epoch from Postgres
  first, under locks inside a transaction, so a token admitted inside the window mints
  nothing that outlives it; above read committed a revocation committed since the
  transaction's snapshot fails it with a serialization error. A WebSocket or Socket.IO
  connection carries what its handshake (or its last reauth) verified to each command, so a
  command that mints re-reads that token's session the same way.
- **Redis down.** A Redis error or timeout makes the same checks from Postgres, so an outage
  never admits a token the fast path would refuse. Tokens issued before the fast path carry
  no `fam` or `pep`, and keep the session-row check until they expire.

### Skip the eligibility read

Every verification also reads the policy principal, to refuse a deactivated one. Pass the same
`RevocationStates` to the authz kernel, and a token the fast path admitted on its principal's
current epoch skips that read too:

```python
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig

states_config = RevocationStates(route="authn-revocations", ttl=timedelta(minutes=5))
authn_kernel = AuthnKernelConfig(..., revocation_states=states_config)
authz_kernel = AuthzKernelConfig(revocation_states=states_config)
```

With it, every write that may deactivate a policy principal raises the principal's epoch in its
transaction and writes it through to Redis after commit, whichever path writes it: the authz
registry's `deactivate_principal`, the authn deactivation, or your own update, bulk patch,
upsert or delete of the `policy_principal_spec` document through its document port. A token
issued before is then refused by its epoch, so the eligibility read has nothing left to catch.

- **What still reads.** Only the check right after the verification, in the same request, about
  the token's own subject, is skipped. Issuing a session (login, refresh, tenant switch) reads
  under the principal's row lock, and a delegated actor is always read, even when it is the
  subject. Tokens issued before the fast path, passwords and API keys read as before.
- **The same rules as revocations.** Such a write refuses to run outside a transaction
  (`authn_revocation_requires_transaction`) or above read committed (`authn_epoch_isolation`),
  and a deactivation whose write-through never lands is enforced within `ttl`.
- **Configure every process that writes policy principals.** A process without the authz
  configuration raises no epoch, and a token it should have ended is admitted until its own
  `exp`; so is one deactivated by a write around the document port (SQL, or another spec over
  the same table). Logins and refreshes still read eligibility, so neither can extend it.
- **Postgres only.** The skip needs the policy principal on Postgres, where an issue's locking
  read excludes a concurrent deactivation; elsewhere every verification reads. The authn and
  authz kernels must carry equal configurations, or the wiring is refused
  (`authn_revocation_states_mismatch`).

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
