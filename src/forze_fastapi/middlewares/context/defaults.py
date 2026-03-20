from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import final
from uuid import UUID

import attrs
from fastapi import Request, Response

from forze.application.execution import CallContext
from forze.base.primitives import uuid7

from .ports import CallContextInjectorPort, CallContextResolverPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DefaultCallContextResolverInjector(
    CallContextResolverPort,
    CallContextInjectorPort,
):
    """Header based call context resolver and injector for FastAPI."""

    exec_header: str = "X-Execution-ID"
    """Header name for the execution id. Only for injection purposes."""

    corr_header: str = "X-Correlation-ID"
    """Header name for the correlation id."""

    caus_header: str = "X-Causation-ID"
    """Header name for the causation id."""

    # ....................... #

    def resolve(self, request: Request) -> CallContext:
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

    def inject(self, response: Response, ctx: CallContext) -> Response:
        response.headers[self.exec_header] = str(ctx.execution_id)
        response.headers[self.corr_header] = str(ctx.correlation_id)

        if ctx.causation_id is not None:
            response.headers[self.caus_header] = str(ctx.causation_id)

        return response
