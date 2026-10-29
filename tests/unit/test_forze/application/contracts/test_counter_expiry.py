"""Counter expiry capability and ``ttl`` validation (``forze.application.contracts.counter``)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze import build_runtime
from forze.application.contracts.counter import (
    COUNTER_TTL_UNSUPPORTED_CODE,
    CounterCapabilities,
    CounterSpec,
    counter_capabilities,
    counter_ttl_unsupported,
    validate_counter_ttl,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #


class TestCounterCapabilities:
    def test_a_port_that_reports_nothing_cannot_expire(self) -> None:
        assert counter_capabilities(object()).expiry is False

    def test_a_report_of_the_wrong_type_reads_as_nothing(self) -> None:
        class _Odd:
            def capabilities(self) -> object:
                return {"expiry": True}

        assert counter_capabilities(_Odd()).expiry is False

    async def test_the_report_is_read_through_the_resolution_wrappers(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            port = runtime.get_context().counter(CounterSpec(name="quota"))

            assert counter_capabilities(port) == CounterCapabilities(expiry=True)


class TestValidateCounterTtl:
    def test_no_ttl_and_a_positive_one_pass(self) -> None:
        validate_counter_ttl(None)
        validate_counter_ttl(timedelta(microseconds=1))

    @pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1)])
    def test_a_ttl_that_has_already_ended_is_a_precondition(self, ttl: timedelta) -> None:
        with pytest.raises(CoreException) as refused:
            validate_counter_ttl(ttl)

        assert refused.value.kind is ExceptionKind.PRECONDITION


def test_the_unsupported_refusal_is_a_configuration_error() -> None:
    error = counter_ttl_unsupported("this backend cannot expire keys")

    assert error.kind is ExceptionKind.CONFIGURATION
    assert error.code == COUNTER_TTL_UNSUPPORTED_CODE
    assert "this backend cannot expire keys" in str(error)
