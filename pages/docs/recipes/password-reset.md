---
title: Self-service password reset
icon: lucide/key-round
summary: Request a reset token, deliver it out of band, and confirm a new password
---

Add the request → deliver → confirm reset flow on top of the
[authn login stack](authn-authz-tenancy-fastapi.md). The operations and routes ship with
the registry; you wire a reset pepper and choose how the token reaches the user.

## The flow

`/auth/password-reset/request` answers a **uniform 202** for known and unknown logins
alike (no account enumeration) and never returns the token;
`/auth/password-reset/confirm` consumes the single-use token (1-hour TTL by default),
sets the new password, and revokes all of the principal's sessions — the same "log out
everywhere" cascade as change-password. Any bad token — wrong, expired, used, superseded
— is a uniform `401`. Only the token's HMAC digest is persisted (a `sensitive` store like
the other credentials); a new request supersedes the previous outstanding one.

## Wire it

Add the reset pepper and the `password_reset` route set on top of the login stack:

```python
AuthnDepsModule(
    kernel=AuthnKernelConfig(
        access_token_secret=secret,
        refresh_token_pepper=refresh_pepper,
        password=PasswordConfig(),
        reset_token_pepper=reset_pepper,  # bytes, ≥ 32 — separate from invite_token_pepper
    ),
    authn={"api": frozenset({"password", "token"})},
    token_lifecycle={"api"},
    password_reset={"api"},
)
```

The two routes attach with the rest of the auth router (`attach_authn_routes`) — no extra
projection step.

## Deliver the token

The raw token must reach the account holder **out of band**, never in the HTTP response.
Set `reset_events` and a successful request stages an `authn.password_reset_requested`
event (`login`, `principal_id`, raw `token`, `expires_at`) onto the standard
[outbox → relay → notify](transactional-notifications.md) pipeline; map it to an
email/SMS command in your notify consumer:

```python
from forze.application.contracts.outbox import OutboxSpec
from forze_kits.aggregates.authn import AuthnPasswordResetRequestedPayload

RESET_EVENTS = OutboxSpec(
    name="authn_events",
    codec=PydanticModelCodec(AuthnPasswordResetRequestedPayload),
    destination=OutboxDestination.queue(route="jobs", channel="notify"),
)
registry = build_authn_registry(AUTH, reset_events=RESET_EVENTS).freeze()
```

Unknown logins stage nothing — the uniform ack is all an outside observer sees.

## Notes

- **The raw token transits the outbox row.** The 1-hour TTL and single-use semantics
  bound the exposure, but treat that store like the credential stores and keep retention
  tight. For zero persistence of the raw token, skip `reset_events` and call
  `ctx.authn.password_reset(spec)` from a custom handler that hands the token straight to
  a mailer.
- **Wire delivery before exposing the route** — without `reset_events` or a custom
  handler, a request mints a token nobody receives.
- **Rate-limit `/auth/password-reset/request`** at the edge: it is an unauthenticated
  write.
