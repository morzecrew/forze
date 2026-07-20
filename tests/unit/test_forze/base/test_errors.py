"""Unit tests for :mod:`forze.base.exceptions`."""

from __future__ import annotations

import pytest
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
    http_status_for_kind,
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
        assert exc.throttled("x").code == "core.throttled"

    def test_throttled_factory_builds_throttled_kind(self) -> None:
        err = exc.throttled("rate limited", code="rate_limited", details={"policy": "p"})
        assert err.kind is ExceptionKind.THROTTLED
        assert err.code == "rate_limited"
        assert err.details == {"policy": "p"}

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


class TestReraiseMappedSite:
    """Every mapped exception carries the interception site in details."""

    def test_site_added_when_mapper_omits_it(self) -> None:
        from forze.base.exceptions._utils import reraise_mapped

        def mapper(e: BaseException, *, site: str, details=None):  # type: ignore[no-untyped-def]
            return exc.infrastructure("backend failed", details={"foo": 1})

        with pytest.raises(CoreException) as exc_info:
            reraise_mapped(mapper, RuntimeError("boom"), site="pg.fetch_one")

        assert exc_info.value.details == {"foo": 1, "site": "pg.fetch_one"}

    def test_site_added_when_details_none(self) -> None:
        from forze.base.exceptions._utils import reraise_mapped

        def mapper(e: BaseException, *, site: str, details=None):  # type: ignore[no-untyped-def]
            return exc.infrastructure("backend failed")

        with pytest.raises(CoreException) as exc_info:
            reraise_mapped(mapper, RuntimeError("boom"), site="pg.fetch_one")

        assert exc_info.value.details == {"site": "pg.fetch_one"}

    def test_mapper_supplied_site_wins(self) -> None:
        from forze.base.exceptions._utils import reraise_mapped

        def mapper(e: BaseException, *, site: str, details=None):  # type: ignore[no-untyped-def]
            return exc.infrastructure("backend failed", details={"site": "inner"})

        with pytest.raises(CoreException) as exc_info:
            reraise_mapped(mapper, RuntimeError("boom"), site="outer")

        assert exc_info.value.details == {"site": "inner"}

    def test_core_exception_passthrough_untouched(self) -> None:
        from forze.base.exceptions._utils import reraise_mapped

        original = exc.not_found("missing")

        with pytest.raises(CoreException) as exc_info:
            reraise_mapped(
                lambda e, *, site, details=None: None,  # type: ignore[arg-type,misc]
                original,
                site="outer",
            )

        assert exc_info.value is original
        assert exc_info.value.details is None


class TestChainFlattening:
    """Regression tests: chaining onto an existing chain must keep later mappers reachable.

    A nested :class:`ChainExceptionMapper` never returns ``None`` — its
    ``__call__`` falls through to ``default_exception`` — so before
    flattening, ``inner.chain(specific)`` made ``specific`` dead code and
    every unmatched exception surfaced as INTERNAL "Unhandled exception"
    (this silently broke every integration package's error mapper, including
    postgres OCC retry on serialization failures).
    """

    @staticmethod
    def _map_value_error(e, *, site, details=None):
        if isinstance(e, ValueError):
            return exc.concurrency("retryable")
        return None

    @staticmethod
    def _fallback(e, *, site, details=None):
        return exc.infrastructure("from-fallback")

    def test_chained_onto_chain_consults_later_mapper(self) -> None:
        # The exact bug shape used by every integration package:
        # default_chain_exc_mapper.chain(<package mapper>).
        outer = default_chain_exc_mapper.chain(self._map_value_error)

        out = outer(ValueError("x"), site="op")

        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_chained_onto_chain_with_fallback_consults_later_mapper(self) -> None:
        inner = ChainExceptionMapper.chain(map_pydantic, fallback=self._fallback)
        outer = inner.chain(self._map_value_error)

        out = outer(ValueError("x"), site="op")

        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY
        assert out.summary != "from-fallback"

    def test_inherited_fallback_applies_when_unmatched(self) -> None:
        inner = ChainExceptionMapper.chain(map_pydantic, fallback=self._fallback)
        outer = inner.chain(self._map_value_error)

        out = outer(RuntimeError("x"), site="op")

        assert out is not None
        assert out.summary == "from-fallback"

    def test_explicit_fallback_overrides_inherited(self) -> None:
        inner = ChainExceptionMapper.chain(map_pydantic, fallback=self._fallback)
        outer = inner.chain(
            self._map_value_error,
            fallback=lambda e, *, site, details=None: exc.infrastructure("explicit"),
        )

        out = outer(RuntimeError("x"), site="op")

        assert out is not None
        assert out.summary == "explicit"

    def test_unmatched_without_fallback_returns_default_exception(self) -> None:
        outer = default_chain_exc_mapper.chain(self._map_value_error)

        out = outer(RuntimeError("x"), site="op")

        assert out is not None
        assert out.kind == ExceptionKind.INTERNAL
        assert out.code == "core.unhandled"

    def test_core_exception_passthrough_unchanged(self) -> None:
        original = exc.not_found("missing")
        outer = default_chain_exc_mapper.chain(self._map_value_error)

        assert outer(original, site="op") is original

    def test_mappers_are_flat_and_order_preserved(self) -> None:
        outer = default_chain_exc_mapper.chain(self._map_value_error)

        assert not any(isinstance(m, ChainExceptionMapper) for m in outer.mappers)
        assert outer.mappers == (map_pydantic, self._map_value_error)

    def test_classmethod_chain_flattens_nested_arguments(self) -> None:
        nested = ChainExceptionMapper.chain(
            ChainExceptionMapper.chain(map_pydantic, fallback=self._fallback),
            self._map_value_error,
        )

        assert nested.mappers == (map_pydantic, self._map_value_error)
        assert nested.fallback is self._fallback


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

        with pytest.raises(CoreException), broken():
            pass


class TestExceptionEgressPolicy:
    def test_not_found_exposes_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.NOT_FOUND).expose_details is True

    def test_authentication_hides_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.AUTHENTICATION).expose_details is False

    def test_infrastructure_hides_details(self) -> None:
        assert exception_egress_policy(ExceptionKind.INFRASTRUCTURE).expose_details is False

    def test_configuration_hides_details(self) -> None:
        # Configuration errors carry internal wiring info and must stay opaque.
        assert exception_egress_policy(ExceptionKind.CONFIGURATION).expose_details is False

    def test_throttled_hides_details_and_is_retryable(self) -> None:
        # Throttle details carry policy/route wiring info; capacity refills over
        # time, so Retry strategies may legitimately wait a rate limit out.
        policy = exception_egress_policy(ExceptionKind.THROTTLED)
        assert policy.expose_details is False
        assert policy.retryable is True

    def test_retryable_kinds_are_exactly_pinned(self) -> None:
        retryable = {
            kind for kind in ExceptionKind if exception_egress_policy(kind).retryable
        }
        assert retryable == {
            ExceptionKind.CONCURRENCY,
            ExceptionKind.INFRASTRUCTURE,
            ExceptionKind.THROTTLED,
        }


class TestHttpStatusForKind:
    def test_client_facing_kinds_map_to_their_status(self) -> None:
        assert http_status_for_kind(ExceptionKind.NOT_FOUND) == 404
        assert http_status_for_kind(ExceptionKind.CONFLICT) == 409
        assert http_status_for_kind(ExceptionKind.CONCURRENCY) == 409
        assert http_status_for_kind(ExceptionKind.VALIDATION) == 422
        assert http_status_for_kind(ExceptionKind.DOMAIN) == 400
        assert http_status_for_kind(ExceptionKind.PRECONDITION) == 400
        assert http_status_for_kind(ExceptionKind.AUTHENTICATION) == 401
        assert http_status_for_kind(ExceptionKind.AUTHORIZATION) == 403
        assert http_status_for_kind(ExceptionKind.THROTTLED) == 429
        assert http_status_for_kind(ExceptionKind.TIMEOUT) == 504

    def test_internal_kinds_fall_back_to_500(self) -> None:
        for kind in (
            ExceptionKind.INTERNAL,
            ExceptionKind.INFRASTRUCTURE,
            ExceptionKind.CONFIGURATION,
        ):
            assert http_status_for_kind(kind) == 500

    def test_every_kind_maps_to_a_valid_http_status(self) -> None:
        for kind in ExceptionKind:
            assert 400 <= http_status_for_kind(kind) <= 599
