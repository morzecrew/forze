"""Unit tests for HTTP endpoint signatures (idempotency header injection)."""

from pydantic import BaseModel

from forze.application.execution import Deps, ExecutionContext, FacadeOpRef
from forze_fastapi.endpoints.http import (
    IDEMPOTENCY_KEY_HEADER,
    BodyAsIsMapper,
    IdempotencyFeature,
)
from forze_fastapi.endpoints.http.composition.signature import (
    build_http_endpoint_signature,
)
from forze_fastapi.endpoints.http.contracts.specs import HttpEndpointSpec

# ----------------------- #


class TestIdempotencyHeaderOnSignature:
    """Regression: idempotent endpoints expose the Idempotency-Key header param."""

    def test_idempotency_header_constant(self) -> None:
        """Header name matches expected wire value."""
        assert IDEMPOTENCY_KEY_HEADER == "Idempotency-Key"

    def test_signature_includes_required_idempotency_header(self) -> None:
        """When IdempotencyFeature is present, FastAPI sees a required header param."""

        class BodyDTO(BaseModel):
            name: str

        class Facade:
            pass

        spec = HttpEndpointSpec(
            http={"method": "POST", "path": "/create"},
            features=[IdempotencyFeature()],
            request={"body_type": BodyDTO},
            response=None,
            mapper=BodyAsIsMapper(BodyDTO),
            facade_type=Facade,
            call=FacadeOpRef(op="test.create"),
        )

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        def facade_dep(_ctx: ExecutionContext) -> Facade:
            return Facade()

        sig = build_http_endpoint_signature(
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
        )
        idem_params = [p for p in sig.parameters.values() if p.name == "__idempotency_key"]
        assert len(idem_params) == 1
        default = idem_params[0].default
        assert getattr(default, "alias", None) == IDEMPOTENCY_KEY_HEADER
