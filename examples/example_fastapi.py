from typing import Any

import attrs
from fastapi import APIRouter, FastAPI, UploadFile
from pydantic import BaseModel, Field

from forze.application.execution import (
    Deps,
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
    UsecasesFacade,
    facade_op,
)
from forze.base.logging import configure_logging, install_excepthook
from forze_fastapi.endpoints.http import (
    HttpEndpointSpec,
    HttpRequestDTO,
    HttpRequestSpec,
    HttpSpec,
    attach_http_endpoint,
    build_http_endpoint_spec,
)

# ----------------------- #

configure_logging(render_mode="console", level="debug")
install_excepthook()

# ....................... #

HELLO_OP = "hello"


class HelloPath(BaseModel):
    check: int


class HelloHeader(BaseModel):
    x_custom_header: str = Field(default="default")


class HelloBody(BaseModel):
    """Form body for POST /hello (optional file + fields)."""

    name: str = Field(default="world", examples=["Ada"])
    biba_boba: int = 10
    some_file: UploadFile | None = None


class HelloInput(BaseModel):
    name: str
    check: int
    different_name: str


class HelloOutput(BaseModel):
    message: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class HelloUsecase(Usecase[HelloInput, HelloOutput]):
    async def main(self, args: HelloInput) -> HelloOutput:
        return HelloOutput(
            message=f"Hello, {args.name}! Check: {args.check}. And different name: {args.different_name}"
        )


def build_registry() -> UsecaseRegistry:
    reg = UsecaseRegistry(
        defaults={
            HELLO_OP: lambda ctx: HelloUsecase(ctx=ctx),
        }
    )
    reg.finalize("example")
    return reg


async def map_hello(
    dto: HttpRequestDTO[HelloPath, Any, HelloHeader, Any, HelloBody],
    /,
    *,
    ctx: ExecutionContext | None = None,
) -> HelloInput:
    assert dto.body is not None
    assert dto.query is not None
    assert dto.header is not None

    return HelloInput(
        name=dto.body.name,
        check=dto.query.check,
        different_name=dto.header.x_custom_header,
    )


class HelloUsecasesFacade(UsecasesFacade):
    hello = facade_op(HELLO_OP, uc=HelloUsecase)


_REGISTRY = build_registry()


def execution_context() -> ExecutionContext:
    return ExecutionContext(deps=Deps())


_ROUTER = APIRouter(prefix="/api", tags=["example"])

_http: HttpSpec = {
    "method": "POST",
    "path": "/hello",
}
_request: HttpRequestSpec[HelloPath, Any, HelloHeader, Any, HelloBody] = {
    "query_type": HelloPath,
    "header_type": HelloHeader,
    "body_type": HelloBody,
    "body_mode": "form",
}

endpoint_spec: HttpEndpointSpec[
    HelloPath,
    Any,
    HelloHeader,
    Any,
    HelloBody,
    HelloInput,
    HelloOutput,
    HelloOutput,
    HelloUsecasesFacade,
] = build_http_endpoint_spec(
    HelloUsecasesFacade,
    HelloUsecasesFacade.hello,
    http=_http,
    request=_request,
    metadata={
        "summary": "Run a tiny usecase (no infra ports)",
    },
    response=HelloOutput,
    mapper=map_hello,
)

attach_http_endpoint(
    _ROUTER,
    spec=endpoint_spec,
    registry=_REGISTRY,
    ctx_dep=execution_context,
)

app = FastAPI(title="Forze FastAPI adapter demo", version="0.0.0")
app.include_router(_ROUTER)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
