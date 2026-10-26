"""Ports for outbound HTTP service integrations."""

from collections.abc import AsyncGenerator, Awaitable
from typing import Any, Protocol, overload, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import StrKey

from .specs import HttpOperationSpec, HttpServiceSpec

# ----------------------- #


@runtime_checkable
class HttpServicePort(Protocol):
    """Port for invoking operations on a configured remote HTTP service."""

    spec: HttpServiceSpec
    """Service specification bound to this port instance."""

    @overload
    def invoke[In: BaseModel, Out: BaseModel](
        self,
        op: HttpOperationSpec[In, Out],
        args: In | None = None,
    ) -> Awaitable[Out]: ...

    @overload
    def invoke(
        self,
        op: StrKey,
        args: BaseModel | None = None,
    ) -> Awaitable[BaseModel]: ...

    def invoke(
        self,
        op: StrKey | HttpOperationSpec[Any, Any],
        args: BaseModel | None = None,
    ) -> Awaitable[Any]:
        """Execute operation ``op`` and return a validated response model.

        Naming the operation by its :class:`HttpOperationSpec` rather than its key is what
        carries the declared types through: the response arrives as the operation's own
        ``return_type`` and the arguments are checked against its ``args_type``, where a key
        can only promise ``BaseModel``. A key remains the right form where the operation is
        chosen at runtime — from config, or from a registry — and there is no declaration to
        name.

        :raises CoreException: ``validation`` when this service declares no such operation.
        """

        ...  # pragma: no cover

    def stream(
        self,
        op: StrKey | HttpOperationSpec[Any, Any],
        args: BaseModel | None = None,
    ) -> AsyncGenerator[bytes]:
        """Execute operation ``op`` and yield the response body as it arrives.

        For a body read incrementally — a streamed completion (server-sent events), a large
        export. The request is built and governed exactly as :meth:`invoke` builds it, and a
        rejected response raises before the first chunk with the exception :meth:`invoke`
        would raise. Nothing is validated against ``return_type``: the bytes are the caller's
        to parse. The invocation deadline travels as a header when the stream opens and does
        not bound the body.

        Closing the generator abandons the call and releases its connection; wrap it in
        ``contextlib.aclosing`` to choose that moment rather than leave it to finalization.

        :raises CoreException: ``validation`` when this service declares no such operation.
        """

        ...  # pragma: no cover
