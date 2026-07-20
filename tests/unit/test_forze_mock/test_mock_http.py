"""MockHttpServicePort: outbound HTTP resolves in-process with zero external services.

Closes the last in-memory gap for deterministic simulation — an app's
``HttpServicePort`` calls are answered by registered handlers, validated against
the operation's args/return models, with no real I/O.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockHttpRegistry
from tests.support.execution_context import context_from_modules

# ----------------------- #


class QuoteArgs(BaseModel):
    symbol: str


class QuoteResult(BaseModel):
    symbol: str
    price: float


class Pong(BaseModel):
    ok: bool = True  # all-optional → an empty body is allowed


SPEC = HttpServiceSpec(
    name="pricing",
    operations={
        "get_quote": HttpOperationSpec(
            name="get_quote",
            method="GET",
            path="/quote",
            args_type=QuoteArgs,
            return_type=QuoteResult,
        ),
        "ping": HttpOperationSpec(
            name="ping",
            method="GET",
            path="/ping",
            args_type=None,
            return_type=Pong,
        ),
    },
)


def _ctx(registry: MockHttpRegistry | None = None):
    return context_from_modules(MockDepsModule(http=registry))


# ....................... #


class TestMockHttpResolves:
    async def test_handler_answers_from_args(self) -> None:
        registry = MockHttpRegistry().on(
            "pricing",
            "get_quote",
            lambda args: QuoteResult(symbol=args.symbol, price=42.0),
        )
        port = _ctx(registry).http.service(SPEC)

        result = await port.invoke("get_quote", QuoteArgs(symbol="ABC"))

        assert isinstance(result, QuoteResult)
        assert result.symbol == "ABC"
        assert result.price == 42.0

    async def test_dict_result_is_coerced_to_return_type(self) -> None:
        registry = MockHttpRegistry().on(
            "pricing",
            "get_quote",
            lambda args: {"symbol": args.symbol, "price": 7.5},
        )
        result = await _ctx(registry).http.service(SPEC).invoke(
            "get_quote", QuoteArgs(symbol="X")
        )
        assert result == QuoteResult(symbol="X", price=7.5)

    async def test_async_handler_is_awaited(self) -> None:
        async def handler(_: BaseModel | None) -> Pong:
            return Pong(ok=True)

        registry = MockHttpRegistry().on("pricing", "ping", handler)
        result = await _ctx(registry).http.service(SPEC).invoke("ping")
        assert isinstance(result, Pong)

    async def test_none_result_yields_empty_body_model(self) -> None:
        registry = MockHttpRegistry().on("pricing", "ping", lambda _: None)
        result = await _ctx(registry).http.service(SPEC).invoke("ping")
        assert isinstance(result, Pong)

    async def test_foreign_basemodel_result_is_coerced(self) -> None:
        class ForeignQuote(BaseModel):  # not QuoteResult, but dump-compatible
            symbol: str
            price: float

        registry = MockHttpRegistry().on(
            "pricing",
            "get_quote",
            lambda args: ForeignQuote(symbol=args.symbol, price=3.0),
        )
        result = await _ctx(registry).http.service(SPEC).invoke(
            "get_quote", QuoteArgs(symbol="Z")
        )
        assert result == QuoteResult(symbol="Z", price=3.0)

    async def test_deterministic_across_calls(self) -> None:
        registry = MockHttpRegistry().on(
            "pricing",
            "get_quote",
            lambda args: QuoteResult(symbol=args.symbol, price=1.0),
        )
        port = _ctx(registry).http.service(SPEC)
        first = await port.invoke("get_quote", QuoteArgs(symbol="A"))
        second = await port.invoke("get_quote", QuoteArgs(symbol="A"))
        assert first == second


class TestMockHttpFailsLoud:
    async def test_unprogrammed_op_raises(self) -> None:
        # The port is wired even with no registry, but every op is unprogrammed.
        port = _ctx().http.service(SPEC)
        with pytest.raises(CoreException) as excinfo:
            await port.invoke("get_quote", QuoteArgs(symbol="A"))
        assert excinfo.value.code == "mock.http.unprogrammed"

    async def test_unknown_op_raises(self) -> None:
        port = _ctx(MockHttpRegistry()).http.service(SPEC)
        with pytest.raises(CoreException):
            await port.invoke("does_not_exist")

    async def test_wrong_args_type_raises(self) -> None:
        registry = MockHttpRegistry().on(
            "pricing", "get_quote", lambda args: QuoteResult(symbol="A", price=1.0)
        )
        port = _ctx(registry).http.service(SPEC)
        with pytest.raises(CoreException):
            await port.invoke("get_quote", Pong())  # not QuoteArgs

    async def test_args_given_to_argless_op_raises(self) -> None:
        registry = MockHttpRegistry().on("pricing", "ping", lambda _: Pong())
        port = _ctx(registry).http.service(SPEC)
        with pytest.raises(CoreException):
            await port.invoke("ping", QuoteArgs(symbol="A"))  # ping takes no args

    async def test_missing_required_body_raises(self) -> None:
        # get_quote's return model has required fields, so a None result is invalid.
        registry = MockHttpRegistry().on("pricing", "get_quote", lambda _: None)
        port = _ctx(registry).http.service(SPEC)
        with pytest.raises(CoreException):
            await port.invoke("get_quote", QuoteArgs(symbol="A"))
