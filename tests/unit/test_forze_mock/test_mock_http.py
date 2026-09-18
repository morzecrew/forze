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
        result = await _ctx(registry).http.service(SPEC).invoke("get_quote", QuoteArgs(symbol="X"))
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
        result = await _ctx(registry).http.service(SPEC).invoke("get_quote", QuoteArgs(symbol="Z"))
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


# ....................... #


class TestMockHttpTakesTheOperationSpec:
    """The typed form, which hands the declaration in instead of its name.

    The behaviour is meant to be identical either way — the only difference is what a type
    checker can say about the result, which no test can assert. What these pin is that the
    runtime path is the same one and that a spec from elsewhere is refused rather than
    resolved by its name.
    """

    async def test_the_declared_spec_invokes_the_same_operation(self) -> None:
        registry = MockHttpRegistry().on(
            "pricing",
            "get_quote",
            lambda args: QuoteResult(symbol=args.symbol, price=42.0),
        )
        port = _ctx(registry).http.service(SPEC)

        result = await port.invoke(SPEC.operations["get_quote"], QuoteArgs(symbol="ABC"))

        assert result == QuoteResult(symbol="ABC", price=42.0)

    async def test_a_bodyless_operation_takes_its_spec(self) -> None:
        registry = MockHttpRegistry().on("pricing", "ping", lambda _: None)

        result = await _ctx(registry).http.service(SPEC).invoke(SPEC.operations["ping"])

        assert isinstance(result, Pong)

    async def test_a_spec_from_another_service_is_refused(self) -> None:
        foreign = HttpOperationSpec(
            name="get_quote",
            method="GET",
            path="/quote",
            args_type=QuoteArgs,
            return_type=Pong,
        )
        registry = MockHttpRegistry().on("pricing", "get_quote", lambda _: None)
        port = _ctx(registry).http.service(SPEC)

        with pytest.raises(CoreException, match="is not the one 'pricing' declares"):
            await port.invoke(foreign, QuoteArgs(symbol="ABC"))

    async def test_registering_by_spec_keys_under_the_operation_name(self) -> None:
        # `on` takes the spec so a type checker can tie the handler's parameter to the
        # declared `args_type`. What it stores is still the name, and getting that wrong
        # would register the handler where nothing looks for it.
        def handler(args: QuoteArgs | None) -> QuoteResult:
            assert args is not None

            return QuoteResult(symbol=args.symbol, price=2.0)

        registry = MockHttpRegistry().on("pricing", SPEC.operations["get_quote"], handler)
        port = _ctx(registry).http.service(SPEC)

        result = await port.invoke(SPEC.operations["get_quote"], QuoteArgs(symbol="ABC"))

        assert result == QuoteResult(symbol="ABC", price=2.0)

    async def test_registering_by_spec_does_not_check_the_service(self) -> None:
        # The registry is keyed by two names and never sees the `HttpServiceSpec`, so a
        # foreign spec registers here; the refusal belongs to the adapter, at the call.
        foreign = HttpOperationSpec(
            name="get_quote",
            method="GET",
            path="/quote",
            args_type=QuoteArgs,
            return_type=Pong,
        )
        registry = MockHttpRegistry().on("pricing", foreign, lambda _: None)

        assert registry.handler_for("pricing", "get_quote") is not None

        with pytest.raises(CoreException, match="is not the one 'pricing' declares"):
            await _ctx(registry).http.service(SPEC).invoke(foreign, QuoteArgs(symbol="A"))

    async def test_a_handler_registered_by_a_foreign_spec_is_never_called(self) -> None:
        # The gap the spec form would otherwise leave: registration keys by name, so a
        # handler type-checked against a *foreign* declaration lands under the local one
        # and is then handed the local args model — the exact mismatch taking the spec was
        # meant to prevent, arriving by a different door. Invoked by the local name here,
        # which is the path the adapter's own foreign-spec refusal does not cover.
        called: list[object] = []

        foreign = HttpOperationSpec(
            name="get_quote",
            method="GET",
            path="/quote",
            args_type=Pong,
            return_type=QuoteResult,
        )

        def handler(args: Pong | None) -> QuoteResult:
            called.append(args)

            return QuoteResult(symbol="never", price=0.0)

        registry = MockHttpRegistry().on("pricing", foreign, handler)
        port = _ctx(registry).http.service(SPEC)

        with pytest.raises(CoreException, match="registered against a different declaration"):
            await port.invoke("get_quote", QuoteArgs(symbol="ABC"))

        assert called == []

    async def test_a_handler_naming_its_own_args_model_is_accepted(self) -> None:
        # The registered signature, not `BaseModel | None`. It used to be refused by a type
        # checker on contravariance, which is why every handler above takes the wide form.
        def handler(args: QuoteArgs | None) -> QuoteResult:
            assert args is not None

            return QuoteResult(symbol=args.symbol, price=1.0)

        registry = MockHttpRegistry().on("pricing", "get_quote", handler)
        port = _ctx(registry).http.service(SPEC)

        result = await port.invoke(SPEC.operations["get_quote"], QuoteArgs(symbol="ABC"))

        assert result == QuoteResult(symbol="ABC", price=1.0)
