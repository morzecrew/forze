"""Deps module registering the in-process resilience executor singleton."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.resilience import (
    CircuitBreakerStore,
    LatencyDigestStore,
    PortPolicy,
    RateLimitStore,
    ResilienceExecutorDepKey,
    ResiliencePortPoliciesDepKey,
    ResilienceSpec,
)
from forze.base.exceptions import ExceptionKind, exc

from forze.application.contracts.deps import Deps
from .executor import InProcessResilienceExecutor
from .policies import builtin_default_policies

# ----------------------- #

_AMBIGUOUS_RETRY_KINDS = frozenset(
    {ExceptionKind.INFRASTRUCTURE, ExceptionKind.TIMEOUT}
)
"""Retry-triggering kinds whose outcome is *ambiguous* — an infrastructure error or a
per-attempt timeout can leave a write applied-but-unacknowledged, so retrying may duplicate
it. Concurrency conflicts and throttles are unambiguous (the call did not take effect), so
they are safe to retry on any method."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ResilienceDepsModule:
    """Register the resilience executor as a process-wide plain singleton."""

    spec: ResilienceSpec | None = None
    """App-provided named-policy catalog merged over :func:`builtin_default_policies`."""

    breaker_store: CircuitBreakerStore | None = None
    """Optional shared breaker store (e.g. Redis). Defaults to process-local."""

    rate_limit_store: RateLimitStore | None = None
    """Optional shared rate-limit store (e.g. Redis), making ``permits/per`` the
    fleet's rate. Defaults to process-local — each replica enforces the rate
    independently, so the fleet-effective rate is ``permits × replicas``."""

    latency_digest_store: LatencyDigestStore | None = None
    """Optional shared adaptive-bulkhead latency digest (e.g. Redis-backed
    DDSketch), making the AIMD congestion signal reflect the fleet's latency.
    Defaults to process-local (windowed P²)."""

    port_policies: tuple[PortPolicy, ...] = attrs.field(
        factory=tuple,
        converter=tuple,
    )
    """Declarative port-level policy bindings: each resolved configurable port
    matching a :class:`~forze.application.contracts.resilience.PortPolicy` key is
    wrapped so its public coroutine methods run under the named policy."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        seen: set[DepKey[Any]] = set()

        for port_policy in self.port_policies:
            if port_policy.key in seen:
                raise exc.configuration(
                    f"Duplicate port policy for dependency key "
                    f"{port_policy.key.name!r}",
                )

            seen.add(port_policy.key)

    # ....................... #

    def __call__(self) -> Deps:
        # Builtin policies are a floor: an app spec may override a named policy
        # (e.g. retune ``occ``) but cannot remove one the framework's own adapters
        # depend on.
        policies = {
            **builtin_default_policies(),
            **(self.spec.policies if self.spec is not None else {}),
        }

        if unknown := sorted(
            str(pp.policy) for pp in self.port_policies if pp.policy not in policies
        ):
            raise exc.configuration(
                "Port policies reference unknown resilience policies: "
                + ", ".join(unknown),
            )

        # A retrying policy applied to *every* method (``methods=None``) will retry writes
        # too. Retrying an ambiguous failure (an infrastructure error or a per-attempt
        # timeout) can duplicate a non-idempotent write, so require the author to opt in per
        # method — list the operations they have confirmed are safe to retry. Concurrency /
        # throttle-only retries (e.g. the ``occ`` policy) are unambiguous and stay unrestricted.
        for pp in self.port_policies:
            if pp.methods is not None:
                continue

            retry = policies[pp.policy].retry

            if retry is None:
                continue

            if ambiguous := sorted(
                kind.value for kind in retry.retry_on & _AMBIGUOUS_RETRY_KINDS
            ):
                raise exc.configuration(
                    f"Port policy for {pp.key.name!r} applies retrying policy "
                    f"{str(pp.policy)!r} (retries {ambiguous}) to every method: this would "
                    "retry a non-idempotent write on an ambiguous failure and risk "
                    "duplicating it. Declare an explicit `methods` list of the operations "
                    "that are safe to retry.",
                    code="resilience.blanket_write_retry",
                )

        # Stores fall back to the executor's process-local defaults when not
        # provided; only pass what was configured so the default Factory wiring
        # (clock injection) stays in one place.
        executor_kwargs: dict[str, Any] = {"policies": policies}

        if self.breaker_store is not None:
            executor_kwargs["breaker_store"] = self.breaker_store

        if self.rate_limit_store is not None:
            executor_kwargs["rate_limit_store"] = self.rate_limit_store

        if self.latency_digest_store is not None:
            executor_kwargs["latency_digest_store"] = self.latency_digest_store

        executor = InProcessResilienceExecutor(**executor_kwargs)
        deps: dict[DepKey[Any], Any] = {ResilienceExecutorDepKey: executor}

        if self.port_policies:
            deps[ResiliencePortPoliciesDepKey] = {
                pp.key: pp for pp in self.port_policies
            }

        return Deps.plain(deps)
