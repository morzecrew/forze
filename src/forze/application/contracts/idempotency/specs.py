from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySpec(BaseSpec):
    """Specification for idempotency behavior."""

    ttl: timedelta = timedelta(hours=24)
    """Dedup window: how long a claim / cached result is retained for replay (default 24h).

    A duplicate that arrives after the TTL **re-executes**, so the TTL must be at least the
    operation's maximum retry / redelivery horizon. For at-least-once queue consumers that
    can redeliver minutes or hours later, set it accordingly. The previous 30s default was
    safe only for immediate synchronous retries and silently let the guarantee lapse for
    async workloads; 24h covers the common cases, but a long-delay queue still needs an
    explicit, larger value."""

    encrypt_result: bool = False
    """Seal the cached operation **result** at rest (default ``False``).

    The idempotency store caches an operation's full return value for replay — potentially
    sensitive business data (e.g. a created order, a user record), in plaintext in a
    Forze-owned store (Redis/Postgres). When ``True``, the result bytes are sealed on commit
    and opened on replay (AAD binds tenant + operation/key); the status/hash metadata stays
    plaintext. Requires a wired keyring (``KeyringDepKey``) — fail-closed otherwise. Records
    written before enabling this still replay (envelope sniff)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
