"""Declarative HTTP operation descriptors."""

from __future__ import annotations

from typing import Any, Generic, Self, TypeVar, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.http import (
    HttpMethod,
    HttpServicePort,
    HttpServiceSpec,
)
from forze.base.exceptions import exc

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class async_http_op(Generic[In, Out]):
    """Descriptor declaring an HTTP operation on :class:`BaseHttpIntegration`."""

    method: HttpMethod
    path: str
    request: type[In] | None = None
    response: type[Out]
    query_from: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset,
    )
    idempotent: bool = False
    site: str | None = None
    allows_empty_body: bool = False
    op_name: str | None = None

    # ....................... #

    @overload
    def __get__(self, obj: None, objtype: type[Any] | None = None) -> Self: ...

    @overload
    def __get__(
        self, obj: BaseHttpIntegration, objtype: type[Any] | None = None
    ) -> HttpBoundOperation[In, Out]: ...

    def __get__(
        self,
        obj: BaseHttpIntegration | None,
        objtype: type[Any] | None = None,
    ) -> Self | HttpBoundOperation[In, Out]:
        if obj is None:
            return self

        if self.op_name is None:
            raise exc.internal("async_http_op is missing op_name on instance binding")

        return HttpBoundOperation(
            port=obj.port,
            op_name=self.op_name,
            return_type=self.response,
        )

    # ....................... #

    def __set_name__(self, owner: type[BaseHttpIntegration], name: str) -> None:
        # Bind the operation name (supplied by the descriptor protocol after
        # construction) by replacing the class attribute with an evolved copy,
        # keeping the descriptor frozen.
        setattr(owner, name, attrs.evolve(self, op_name=name))


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpBoundOperation(Generic[In, Out]):
    """Bound HTTP operation callable on a :class:`BaseHttpIntegration` instance."""

    port: HttpServicePort
    op_name: str
    return_type: type[Out]

    # ....................... #

    async def __call__(self, args: In | None = None) -> Out:
        result = await self.port.invoke(self.op_name, args)

        if isinstance(result, self.return_type):
            return result

        return self.return_type.model_validate(result, from_attributes=True)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseHttpIntegration:
    """Base class for declarative outbound HTTP service facades."""

    port: HttpServicePort
    spec: HttpServiceSpec

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.port.spec.name != self.spec.name:
            raise exc.configuration(
                "HttpServicePort spec name does not match BaseHttpIntegration spec",
            )
