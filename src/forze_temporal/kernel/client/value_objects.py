from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from collections.abc import Mapping
from datetime import timedelta
from typing import Any, final

import attrs
from pydantic import SecretStr
from temporalio.client import Interceptor, TLSConfig
from temporalio.common import (
    RetryPolicy,
    TypedSearchAttributes,
    WorkflowIDReusePolicy,
)
from temporalio.converter import DataConverter

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


def _optional_secret_converter(v: str | SecretStr | None) -> SecretStr | None:
    if v is None:
        return None

    return pydantic_secret_converter(v)


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class TemporalConfig:
    """Temporal configuration."""

    namespace: str = "default"
    """Namespace to use for the client."""

    lazy: bool = False
    """Whether to lazy initialize the client."""

    interceptors: list[Interceptor] | None = attrs.field(default=None)
    """Interceptors to apply to the client."""

    tls: bool | TLSConfig = False
    """TLS for the gRPC connection: ``True`` for default TLS, a
    :class:`temporalio.client.TLSConfig` for mTLS / custom roots, ``False``
    for plaintext (default, matches previous behavior)."""

    api_key: SecretStr | None = attrs.field(
        default=None,
        converter=_optional_secret_converter,
        repr=False,
    )
    """API key sent as the gRPC bearer credential (e.g. Temporal Cloud).
    Requires ``tls`` to be enabled."""

    data_converter: DataConverter | None = attrs.field(default=None, repr=False)
    """Data converter override. ``None`` (default) uses the pydantic data
    converter, matching previous behavior. Supply a custom converter to
    install e.g. an encrypting payload codec."""

    rpc_metadata: Mapping[str, str] | None = None
    """Extra headers attached to every RPC call."""

    encrypt_payloads: bool = False
    """Seal workflow/activity payloads at rest with the wired keyring (single-key BYOK).
    When ``True``, the startup hook composes an encrypting ``PayloadCodec`` over
    :attr:`data_converter` and fails closed if no keyring is registered. Default ``False``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.api_key is not None and not self.tls:
            raise exc.configuration(
                "Temporal api_key requires TLS: set tls=True or provide a TLSConfig"
            )


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class TemporalStartOptions:
    """Per-workflow-kind start options, passed through to ``Client.start_workflow``.

    Engine vocabulary on purpose. These are properties of a workflow *kind* — "pipeline
    runs never retry", "reports time out at 6 h" — not of a call site, so they are
    declared once on :class:`~forze_temporal.TemporalWorkflowConfig` beside the task
    queue, and the engine-agnostic ``DurableWorkflowCommandPort`` never learns about
    them. The Temporal adapter accepts an optional per-call set that
    :meth:`override`\\ s the configured one field by field.

    Every field defaults to ``None`` meaning *unspecified*: an unset field is omitted
    from the SDK call entirely rather than sent as a default, so wiring that declares no
    options produces exactly the request earlier releases sent.

    Anything beyond this list — versioning ramps, typed search-attribute *updates*,
    eager start — is what :attr:`~forze_temporal.TemporalClientPort.native` is for.
    """

    retry_policy: RetryPolicy | None = None
    """Attempt ceiling and backoff for the whole workflow (not for its activities)."""

    execution_timeout: timedelta | None = None
    """Wall-clock budget for the *whole* execution, retries and continue-as-new included."""

    run_timeout: timedelta | None = None
    """Budget for a single run; a longer :attr:`execution_timeout` lets a retry follow."""

    task_timeout: timedelta | None = None
    """Budget for one workflow task — how long a worker may hold it before it is retried."""

    id_reuse_policy: WorkflowIDReusePolicy | None = None
    """What a start may do when a *closed* run already holds the workflow id."""

    memo: Mapping[str, Any] | None = None
    """Non-indexed metadata carried on the run and returned by describe."""

    search_attributes: TypedSearchAttributes | None = None
    """Indexed attributes for visibility queries. Keys must already be registered on the
    namespace — Temporal rejects a start naming an unregistered attribute."""

    start_delay: timedelta | None = None
    """Hold the first workflow task for this long. Mutually exclusive with a cron
    schedule, which this option set deliberately does not carry."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for name in ("execution_timeout", "run_timeout", "task_timeout"):
            value: timedelta | None = getattr(self, name)

            # Temporal reads a non-positive timeout as "unset", so it would silently
            # widen the bound the caller thought they were tightening.
            if value is not None and value.total_seconds() <= 0:
                raise exc.configuration(
                    f"Temporal {name} must be positive, got {value}",
                    code="core.temporal.start_options_invalid",
                )

        if self.start_delay is not None and self.start_delay.total_seconds() < 0:
            raise exc.configuration(
                f"Temporal start_delay cannot be negative, got {self.start_delay}",
                code="core.temporal.start_options_invalid",
            )

    # ....................... #

    def override(self, other: "TemporalStartOptions | None") -> "TemporalStartOptions":
        """Merge *other* over this set, field by field.

        Only the fields *other* actually sets win; everything else keeps this set's
        value. That is what makes a per-call override usable — a caller changing one
        timeout does not have to restate the workflow kind's whole configuration, and
        cannot silently drop the parts they did not mention.
        """

        if other is None:
            return self

        changed = {
            field.name: value
            for field in attrs.fields(TemporalStartOptions)
            if (value := getattr(other, field.name)) is not None
        }

        return attrs.evolve(self, **changed)

    # ....................... #

    def as_start_kwargs(self) -> dict[str, Any]:
        """The set fields, as ``Client.start_workflow`` keyword arguments.

        Unset fields are absent from the mapping rather than present as ``None``: the
        SDK's own defaults for ``id_reuse_policy`` and friends are not ``None``, so
        forwarding them would change behaviour for callers who configured nothing.
        """

        kwargs: dict[str, Any] = {}

        for field in attrs.fields(TemporalStartOptions):
            value = getattr(self, field.name)

            if value is not None:
                kwargs[field.name] = value

        return kwargs
