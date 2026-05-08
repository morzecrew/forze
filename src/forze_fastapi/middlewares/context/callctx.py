from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import final
from uuid import UUID

import attrs
from fastapi import Request

from forze.application.execution import CallContext
from forze.base.primitives import uuid7

from .ports import CallContextCodecPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HeaderCallContextCodec(CallContextCodecPort):
    """Header based call context codec for FastAPI."""

    exec_header: str = "X-Request-ID"
    """Header name for the execution id. Only for injection purposes."""

    corr_header: str = "X-Correlation-ID"
    """Header name for the correlation id."""

    caus_header: str = "X-Causation-ID"
    """Header name for the causation id."""

    # ....................... #

    def decode(self, request: Request) -> CallContext:
        execution_id = uuid7()
        corr_raw = request.headers.get(self.corr_header)
        caus_raw = request.headers.get(self.caus_header)

        correlation_id = UUID(corr_raw) if corr_raw else uuid7()
        causation_id = UUID(caus_raw) if caus_raw else None

        return CallContext(
            execution_id=execution_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    # ....................... #

    def encode(
        self,
        headers: list[tuple[bytes, bytes]],
        ctx: CallContext,
    ) -> list[tuple[bytes, bytes]]:
        """Inject the call context headers into the FastAPI response.

        Here we use pure ASGI format to avoid corrupting the response.
        For more details check the implementation of
        :class:`forze_fastapi.middlewares.context.middleware.ContextBindingMiddleware`
        """

        headers.extend(
            [
                (
                    self.exec_header.encode("latin-1"),
                    str(ctx.execution_id).encode("latin-1"),
                ),
                (
                    self.corr_header.encode("latin-1"),
                    str(ctx.correlation_id).encode("latin-1"),
                ),
            ]
        )

        if ctx.causation_id is not None:
            headers.append(
                (
                    self.caus_header.encode("latin-1"),
                    str(ctx.causation_id).encode("latin-1"),
                )
            )

        return headers
