from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Callable, Final, Mapping
from uuid import UUID

import attrs
from temporalio.api.common.v1 import Payload

from forze.application.execution import CallContext, PrincipalContext
from forze.base.primitives import uuid7

# ----------------------- #

_EXEC_HEADER: Final[str] = "Forze-Execution-ID"
_CORR_HEADER: Final[str] = "Forze-Correlation-ID"
_TENANT_HEADER: Final[str] = "Forze-Tenant-ID"
_ACTOR_HEADER: Final[str] = "Forze-Actor-ID"
_ENCODING: Final[str] = "utf-8"

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalDecodedContext:
    execution_id: UUID | None = attrs.field(default=None)
    correlation_id: UUID | None = attrs.field(default=None)
    tenant_id: UUID | None = attrs.field(default=None)
    actor_id: UUID | None = attrs.field(default=None)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalContextCodec:
    """Codec for encoding and decoding temporal context."""

    def encode(
        self,
        *,
        call: CallContext | None = None,
        principal: PrincipalContext | None = None,
    ) -> Mapping[str, Payload]:
        headers: dict[str, Payload] = {}

        if call is not None:
            headers[_EXEC_HEADER] = Payload(
                data=str(call.execution_id).encode(_ENCODING)
            )
            headers[_CORR_HEADER] = Payload(
                data=str(call.correlation_id).encode(_ENCODING)
            )

            # We don't encode causation id here

        if principal is not None:
            if principal.tenant_id is not None:
                headers[_TENANT_HEADER] = Payload(
                    data=str(principal.tenant_id).encode(_ENCODING)
                )

            if principal.actor_id is not None:
                headers[_ACTOR_HEADER] = Payload(
                    data=str(principal.actor_id).encode(_ENCODING)
                )

        return headers

    # ....................... #

    def decode(
        self,
        headers: Mapping[str, Payload],
    ) -> TemporalDecodedContext:
        exec_raw = headers.get(_EXEC_HEADER)
        corr_raw = headers.get(_CORR_HEADER)
        tenant_raw = headers.get(_TENANT_HEADER)
        actor_raw = headers.get(_ACTOR_HEADER)

        return TemporalDecodedContext(
            execution_id=UUID(exec_raw.data.decode(_ENCODING)) if exec_raw else None,
            correlation_id=UUID(corr_raw.data.decode(_ENCODING)) if corr_raw else None,
            tenant_id=UUID(tenant_raw.data.decode(_ENCODING)) if tenant_raw else None,
            actor_id=UUID(actor_raw.data.decode(_ENCODING)) if actor_raw else None,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalContextBinder:
    """Build local execution context from decoded Temporal headers."""

    execution_id_factory: Callable[[], UUID] = attrs.field(default=uuid7)

    # ....................... #

    def bind(
        self,
        decoded: TemporalDecodedContext,
    ) -> tuple[CallContext, PrincipalContext]:
        execution_id = self.execution_id_factory()
        correlation_id = decoded.correlation_id or execution_id

        call = CallContext(
            execution_id=execution_id,
            correlation_id=correlation_id,
            causation_id=decoded.execution_id,
        )

        principal = PrincipalContext(
            tenant_id=decoded.tenant_id,
            actor_id=decoded.actor_id,
        )

        return call, principal
