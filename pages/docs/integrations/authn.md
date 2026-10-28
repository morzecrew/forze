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
by profile name; override the defaults via `token_verifiers`, `resolvers`, etc. Each override
must name a route in `authn`, and a verifier override a method that route enables; any
other is refused when the module is built. A `token_verifiers` override also needs a
`resolvers` override on its route: the default resolver would take an external token's
subject as an internal principal id.

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
in Redis, reading Postgres only for what Redis misses. With `key_mac` too, every live cached
state is signed, and API keys are checked the same way, from a state cached under the key's
digest. The eligibility read stays for both, except that a token's is skipped when the authz
kernel carries the same configuration ([below](#skip-the-eligibility-read)).

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
        key_mac=StateMacKeys(active=mac_key),  # optional: signed states, API keys too
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

- **Without `key_mac`, Redis is trusted for sessions.** The session states and epochs then
  carry no MAC, so whoever can write the states' namespace can restore a `live` state or an
  older epoch, and a revoked token is admitted until its own `exp`. Let nothing but the
  identity plane write that namespace. With `key_mac`, each epoch and live family or session
  state carries a MAC and a not-after, as an API key's state does ([below](#api-keys)): one
  forged, copied from another principal or session, or cached before `key_mac` was set is
  read from Postgres, and one replayed over a newer state stops verifying `ttl` after it was
  cached. Its MAC covers the states route, the principal and the state key, not the
  namespace, so give every deployment its own `key_mac`.
- **Transactions are required.** With the fast path on, issuing and revoking sessions refuse
  to run outside a transaction (`authn_revocation_requires_transaction`), so keep `tx_route`
  set on both registries. They refuse the same way, before writing, in a transaction that does
  not hold every store they write (the epochs, and for a deactivation or a password change the
  credentials and sessions too): a store on another database would commit on its own. The
  Postgres stores report it; one that cannot is not checked. Issue and logout serialize on the
  epoch's row on Postgres only, with the revocation at read committed (the default); a
  revocation at repeatable read or serializable is refused (`authn_epoch_isolation`). On
  Postgres, a raise of the epoch (a logout everywhere, a password change or reset) first locks
  the policy principal's row, as an issue does before it holds the epoch, so a deactivation of
  that row later in the same transaction, through SQL or its document port, waits for a
  concurrent login instead of deadlocking with it. The authn deactivation takes the
  credentials first and ends with `revoke_tokens` itself, so do not call `revoke_tokens` before
  it. Revoking an API key needs no transaction: its state is written once the key's row has
  committed, whichever way it runs.
- **`session_binding`.** `"session"` (the default) refuses a token once a refresh replaced its
  session, as without the fast path; `"family"` admits it until its own `exp` while the
  family is live. A logout, a family revocation and refresh-token reuse refuse it either way.
- **Minting re-reads the credential.** A session, an API key, a password account or an invite
  issued in a request that authenticated with an access token re-reads that token's session and
  epoch from Postgres first, whoever it is issued for, so a token admitted inside the window
  mints nothing that outlives it. The same mints in a request that authenticated with an API key
  (with `key_mac` set) re-read the key's row the same way (`api_key_revoked`). A session is
  issued only inside a transaction (above), where the re-read takes locks; above read committed,
  a revocation committed since the transaction's snapshot fails it with a serialization error.
  An API key, a password account or an invite can be minted outside one, `tx_route=None`
  included: the re-read then takes no lock but reads the latest commit, and a revocation
  committing after it is ordered after the mint, which it leaves in place, as it would a locked
  mint it waited for. Revoking a credential never revokes what was minted from it before. A
  WebSocket or Socket.IO connection carries what its handshake (or its last reauth) verified to
  each command, so a command that mints re-reads that credential the same way: once a revocation
  ends it, or a refresh elsewhere rotates the token's session (under `"session"` binding), the
  socket's mints are refused until it reauths.
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
refuses one equal to any of its peppers, its token secret or an HS256 signer's secret
(`authn_state_mac_key_reused`). It cannot see a key held by an asymmetric or KMS-backed signer;
what makes any reuse harmless is that each MAC key is used only through a key derived from it.

A states store of your own needs `ApiKeyStatesPort` as well as `RevocationStatesPort` for
API keys: with `key_mac` set, the build refuses one without it
(`authn_api_key_states_unsupported`). The Redis and mock stores implement both.

`StateMacKeys` takes keys of at least 32 bytes. To rotate one, make the new key `active`
and move the old one to `previous`, on every process that verifies or revokes; after `ttl`,
when every state signed under the old key has expired, drop it. A state signed under a key
no longer configured is read from Postgres, so dropping it early costs reads, not
admissions. A key carries no tenant, cached or not, and a deactivated principal's keys are
still refused, by the eligibility read or, where it is [skipped](#skip-the-eligibility-read),
by the epoch the deactivation raised, except within the replay window under
[The window](#the-window).

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
- **Whoever can write Redis replayed a state cached before the revocation.** With `key_mac`,
  it verifies until its not-after, `ttl` after it was cached. Nothing logs it. Without
  `key_mac`, nothing bounds a replay but the token's own `exp`, as said above.

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

### Skip the eligibility read

Every verification also reads the policy principal, to refuse a deactivated one. Pass the same
`RevocationStates` to the authz kernel, and a token the fast path admitted on its principal's
current epoch skips that read too, as does an API key whose cached state recorded that epoch:

```python
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig

states_config = RevocationStates(
    route="authn-revocations",
    ttl=timedelta(minutes=5),
    key_mac=StateMacKeys(active=mac_key),
)
authn_kernel = AuthnKernelConfig(..., revocation_states=states_config)
authz_kernel = AuthzKernelConfig(revocation_states=states_config)
```

With it, every write that may deactivate a policy principal raises the principal's epoch in its
transaction and writes it through to Redis after commit, whichever path writes it: the authz
registry's `deactivate_principal`, the authn deactivation, or your own update, bulk patch,
upsert or delete of the `policy_principal_spec` document through its document port. A token
issued before is then refused by its epoch, so the eligibility read has nothing left to catch.

- **Signed epochs only.** The skip trusts the cached epoch, so the authz kernel refuses
  `revocation_states` without `key_mac` (`authn_eligibility_skip_requires_key_mac`), and a
  `PolicyPrincipalEpochHook` registered by hand without it leaves every verification reading.
  Unsigned, an older epoch restored in Redis would admit a deactivated principal's tokens until
  their `exp`.
- **What still reads.** Only the check right after the verification, in the same request, about
  the token's own subject, is skipped. Issuing a session (login, refresh, tenant switch) reads
  under the principal's row lock, and a delegated actor is always read, even when it is the
  subject. Tokens issued before the fast path and passwords read as before.
- **API keys.** A key's state written on a miss records the principal's epoch, read before the
  principal is found active; a later verification whose state carries the principal's current
  cached epoch skips the read, and reads neither the key's row nor the principal. The miss itself
  reads, and so does a key whose principal's epoch has moved since, whose principal was inactive
  when its state was written, or whose state an earlier release wrote. During a rolling deploy,
  a process of an earlier release reads such a state as a miss, from the key's row, until it
  expires.
  - **Inside a transaction, a key always reads.** The transaction may have raised the epoch
    itself, which reaches the cache only after commit.
  - **A missing cached epoch.** The verification compares the database's epoch instead and
    writes it back: as sound, since a deactivation raises the epoch in its own transaction, and
    one read where the eligibility check would make one. If that read fails, it reads
    eligibility.
  - **Two Redis reads.** A key's state and its principal's epoch sit on different Redis Cluster
    slots, so a warm verification reads them one after the other, each bounded by `timeout`:
    twice `timeout` at worst before it falls back to the eligibility read.
- **The same rules as revocations.** Such a write refuses to run outside a transaction, or in
  one that holds neither its own write nor the epoch's (a scope on another route or another
  database), before it writes (`authn_revocation_requires_transaction`); it refuses above read
  committed too (`authn_epoch_isolation`). A deactivation whose write-through never lands is
  enforced within `ttl`. For a logout and a deactivation in one transaction, see
  **Transactions are required** ([above](#check-sessions-and-api-keys-from-redis)).
- **Configure every process that writes policy principals.** A process without the authz
  configuration raises no epoch, and a token it should have ended is admitted until its own
  `exp`, an API key until its cached state expires (`ttl`); so is one deactivated by a write
  around the document port (SQL, or another spec over
  the same table), or one deactivated before you turned this on through a path that raised no
  epoch (the authz registry alone). Logins and refreshes still read eligibility, so none of
  these can be extended. Keep the policy principals on the database whose transaction holds the
  epochs, so a deactivation and its raise commit together.
- **Postgres only.** The skip needs the policy principal on Postgres, where an issue's locking
  read excludes a concurrent deactivation, and the principal epochs there too, so a raise is
  held to the deactivation's transaction; elsewhere, the mock included, every verification
  reads. Without the authz configuration every verification reads, as before. With a
  different one, building the eligibility gate (on the first authentication or issue) is
  refused (`authn_revocation_states_mismatch`).

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
  the issue; Mongo's locking read takes no lock, so there the race remains. A password change,
  a reset and a deactivation likewise read the password account with a row lock on Postgres,
  right before they write it, and hash before they take it: of a change or reset and a
  deactivation, the second waits, so the deactivation closes the changed account, or the
  change or reset reads it closed and is refused. The lock goes to the `write` table of the
  password-account route when it differs from `read`, and otherwise to `read`, which must then
  accept `FOR UPDATE`: a table or a single-table view does, a view that joins or groups does
  not, as on a read-only route over such a view. Without the lock (outside a transaction, or on Mongo) a deactivation whose close of
  the password account or of an API key a concurrent write made stale reads it again and
  closes it, a few times at most; a conflict the backend itself reports still fails the
  transaction.
  `tx_route=None` runs the writes without a transaction and gives up these guarantees.
  Refresh refuses to run inside a transaction the caller already opened: a reused refresh
  token's family is revoked in a transaction of its own, which an enclosing one would roll
  back.
- **Overlapping refreshes:** presenting a rotated refresh token again is reuse, and its
  whole family is revoked. Tabs sharing one refresh cookie hit that on a benign race.
  `AuthnKernelConfig(refresh_token=RefreshTokenConfig(reuse_grace=timedelta(seconds=10)))`
  refuses a token rotated within the grace, while its successor is unused, with the
  authentication error `refresh_superseded` and revokes nothing. A refresh that loses the
  rotation to a concurrent one gets the same error where the store detects the stale write
  itself, as Postgres does; a backend that aborts the transaction on the conflict, as Mongo
  does, still answers with the conflict. Either way the client refreshes again with the
  token it now holds. A stolen token replayed within the grace mints nothing, and
  its holder learns only that it was just rotated, but it raises `REFRESH_SUPERSEDED`, not
  the `REFRESH_REUSE_DETECTED` theft signal, so keep the grace to seconds. It must be
  shorter than the refresh lifetime; the default `0` keeps strict reuse detection.
- **Issued API keys:** `issue_api_key` returns an `IssuedApiKey`; its raw key, shown only
  at issuance, is `issued.secret`. It stays out of `repr()` and `str()`, so logging the
  result does not log the key.
- **Browser clients:** the generated login/refresh/logout routes can carry their
  tokens in `HttpOnly` cookies instead of response bodies — see
  [cookie mode](../reference/fastapi-routes.md#cookie-mode). Logout revokes the
  sessions from the access cookie's identity or, without one, the refresh cookie (the
  `logout_by_refresh_token` operation, through
  `TokenLifecyclePort.revoke_tokens_by_refresh_token`). The access cookie expires
  first, so the refresh cookie's path must cover the logout route; `runtime_lifespan`
  refuses to start otherwise.
- Authorization is a sibling plane — `forze_identity.authz` (`AuthzDepsModule`).
- The conceptual model (verify → resolve, resolver flavors) is in
  [Identity & access](../identity-tenancy-enc/identity.md); this page is the wiring.
- For external IdPs, pair this with the [OIDC](oidc.md) verifier.
