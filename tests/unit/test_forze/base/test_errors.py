"""Unit tests for :mod:`forze.base.exceptions`."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from forze.base.exceptions import (
    ChainExceptionMapper,
    CoreException,
    ExceptionInterceptor,
    ExceptionKind,
    default_chain_exc_mapper,
    exc,
    exception_egress_policy,
    map_pydantic,
)

# ----------------------- #


class TestCoreException:
    def test_str_includes_kind_code_and_summary(self) -> None:
        err = exc.not_found("Something went wrong", code="oops")
        assert "Not_found" in str(err) or "not_found" in str(err).lower()
        assert "oops" in str(err)
        assert "Something went wrong" in str(err)

    def test_default_code_per_kind(self) -> None:
        assert exc.internal("x").code == "core.internal"
        assert exc.not_found("x").code == "core.not_found"
        assert exc.conflict("x").code == "core.conflict"
        assert exc.validation("x").code == "core.validation"
        assert exc.infrastructure("x").code == "core.infrastructure"

    def test_custom_code_and_details(self) -> None:
        err = exc.conflict("mismatch", code="rev", details={"rev": 1})
        assert err.kind == ExceptionKind.CONFLICT
        assert err.summary == "mismatch"
        assert err.code == "rev"
        assert err.details == {"rev": 1}

    def test_enrich_returns_copy_with_details(self) -> None:
        err = exc.domain("bad state")
        enriched = err.enrich(resource={"id": "1"})
        assert enriched is not err
        assert enriched.summary == err.summary
        assert enriched.details is not None


class TestMapPydantic:
    def test_maps_validation_error(self) -> None:
        class M(BaseModel):
            x: int

        try:
            M.model_validate({"x": "nope"})
        except PydanticValidationError as e:
            out = map_pydantic(e, site="validate")
        else:
            pytest.fail("expected pydantic validation error")

        assert out is not None
        assert out.kind == ExceptionKind.VALIDATION
        assert out.code == "pydantic.validation"
        assert out.details is not None
        assert "errors" in out.details
        assert "input" not in out.details["errors"][0]

    def test_returns_none_for_other_exceptions(self) -> None:
        assert map_pydantic(RuntimeError("x"), site="op") is None


class TestDefaultChainMapper:
    def test_chains_pydantic_mapper(self) -> None:
        class M(BaseModel):
            x: int

        try:
            M.model_validate({"x": "bad"})
        except PydanticValidationError as e:
            out = default_chain_exc_mapper(e, site="op")
        else:
            pytest.fail("expected validation error")

        assert out is not None
        assert out.kind == ExceptionKind.VALIDATION


class TestExceptionInterceptor:
    @pytest.mark.asyncio
    async def test_coroutine_maps_unknown_to_core(self) -> None:
        mapper = ChainExceptionMapper.chain(
            lambda e, *, site, details: exc.internal(f"{site}:{e}", code="wrapped")
        )
        interceptor = ExceptionInterceptor(mapper=mapper)

        @interceptor.coroutine(site="my_op")
        async def boom() -> None:
            raise RuntimeError("fail")

        with pytest.raises(CoreException, match="my_op:fail") as info:
            await boom()
        assert info.value.code == "wrapped"

    @pytest.mark.asyncio
    async def test_coroutine_passthrough_core(self) -> None:
        interceptor = ExceptionInterceptor(mapper=default_chain_exc_mapper)

        @interceptor.coroutine(site="op")
        async def raise_core() -> None:
            raise exc.not_found("missing")

        with pytest.raises(CoreException, match="missing"):
            await raise_core()

    def test_function_intercepts_sync(self) -> None:
        interceptor = ExceptionInterceptor(mapper=default_chain_exc_mapper)

        @interceptor.function(site="sync")
        def bad() -> int:
            raise ValueError("nope")

        with pytest.raises(CoreException):
            bad()

    def test_contextmanager_intercepts_enter_failure(self) -> None:
        from contextlib import contextmanager

        interceptor = ExceptionInterceptor(mapper=default_chain_exc_mapper)

        @interceptor.contextmanager(site="cm")
        @contextmanager
        def broken() -> __import__("typing").Generator[None, None, None]:
            raise ValueError("enter")
            yield

        with pytest.raises(CoreException):
            with broken():
                pass


class TestExceptionEgressPolicy:
    def test_not_found_exposes_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.NOT_FOUND).expose_details is True

    def test_authentication_hides_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.AUTHENTICATION).expose_details is False

    def test_infrastructure_hides_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.INFRASTRUCTURE).expose_details is False
