"""Unit tests for the logging front-door helpers and volume controls."""

import io
import json
import logging

import pytest
from structlog import DropEvent

from forze._logging import ForzeLogger
from forze.base.logging import (
    DEFAULT_HEALTH_PATHS,
    AccessLogMode,
    AccessLogSampler,
    Logger,
    LoggerAware,
    bootstrap_logging,
    get_logger,
    resolve_logger,
)
from forze.base.logging.constants import INTEGRATION_LOGGER_PREFIX
from forze.base.logging.logger import _integration_logger
from forze.base.logging.processors import SamplingDeduplicator
from tests.support.logging import reset_forze_stdlib_loggers


@pytest.fixture(autouse=True)
def _reset_logging():
    yield
    # ``third.party`` is a foreign logger configured by one of the bootstrap tests.
    reset_forze_stdlib_loggers("third.party")


def _json_records(stream: io.StringIO) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if line.strip().startswith("{")
    ]


class TestGetLogger:
    def test_returns_logger_with_name(self) -> None:
        log = get_logger("myapp.orders")

        assert isinstance(log, Logger)
        assert log.name == "myapp.orders"

    def test_accepts_str_enum(self) -> None:
        assert get_logger(ForzeLogger.APPLICATION).name == "forze.application"


class TestResolveLogger:
    def test_default_is_per_domain_integration_logger(self) -> None:
        log = resolve_logger(None, domain="cache")

        assert log.name == f"{INTEGRATION_LOGGER_PREFIX}.cache"

    def test_override_wins(self) -> None:
        override = Logger("forze_postgres.adapters")

        assert resolve_logger(override, domain="cache") is override

    def test_default_is_memoized_per_domain(self) -> None:
        assert resolve_logger(None, domain="doc") is resolve_logger(None, domain="doc")
        assert _integration_logger("doc") is not _integration_logger("other")

    def test_enum_prefix_matches_base_constant(self) -> None:
        assert str(ForzeLogger.INTEGRATIONS) == INTEGRATION_LOGGER_PREFIX


class TestLoggerAware:
    def test_defaults_to_supplied_default(self) -> None:
        default = Logger("pkg.adapters")

        assert LoggerAware().logger_or(default) is default

    def test_override_wins(self) -> None:
        default = Logger("pkg.adapters")
        override = Logger("pkg.adapters.tenant")

        assert LoggerAware(logger=override).logger_or(default) is override


class TestSamplingDeduplicator:
    def test_passthrough_without_control_keys(self) -> None:
        proc = SamplingDeduplicator()
        event = {"logger_name": "x", "event": "hi", "level": "info"}

        assert proc(None, "info", dict(event)) == event

    def test_sample_keeps_one_in_n(self) -> None:
        proc = SamplingDeduplicator()
        kept = 0

        for _ in range(9):
            try:
                proc(None, "info", {"logger_name": "x", "event": "e", "_sample": 3})
                kept += 1
            except DropEvent:
                pass

        assert kept == 3

    def test_dedup_emits_once_per_window(self) -> None:
        proc = SamplingDeduplicator(default_window=1000.0)
        kept = 0

        for _ in range(5):
            try:
                proc(None, "warning", {"event": "flap", "_dedup_key": "flap"})
                kept += 1
            except DropEvent:
                pass

        assert kept == 1

    def test_control_keys_are_stripped(self) -> None:
        proc = SamplingDeduplicator()

        out = proc(
            None,
            "info",
            {"event": "e", "_sample": 1, "_dedup_key": "k", "_dedup_window": 5},
        )

        assert "_sample" not in out
        assert "_dedup_key" not in out
        assert "_dedup_window" not in out


class TestAccessLogSampler:
    def test_excluded_subject_never_logged(self) -> None:
        sampler = AccessLogSampler(exclude=DEFAULT_HEALTH_PATHS)

        assert not sampler.should_log(subject="/healthz", is_error=False)
        assert not sampler.should_log(subject="/healthz", is_error=True)

    def test_errors_always_logged_under_sampling(self) -> None:
        sampler = AccessLogSampler(sample_rate=10)

        assert all(sampler.should_log(subject="/x", is_error=True) for _ in range(5))

    def test_successes_are_sampled_one_in_n(self) -> None:
        sampler = AccessLogSampler(sample_rate=10)

        kept = sum(sampler.should_log(subject="/x", is_error=False) for _ in range(100))

        assert kept == 10

    @pytest.mark.parametrize("rate", [1, 0, -1])
    def test_sample_rate_one_or_less_keeps_all(self, rate: int) -> None:
        # A rate of 1 (or less) means "no sampling" — every success is logged, not none.
        sampler = AccessLogSampler(sample_rate=rate)

        assert all(sampler.should_log(subject="/x", is_error=False) for _ in range(5))

    def test_full_mode_logs_every_success(self) -> None:
        sampler = AccessLogSampler(mode=AccessLogMode.FULL)

        assert all(sampler.should_log(subject="/x", is_error=False) for _ in range(5))

    def test_off_mode_logs_nothing_even_errors(self) -> None:
        sampler = AccessLogSampler(mode=AccessLogMode.OFF)

        assert not sampler.should_log(subject="/x", is_error=True)


class TestBootstrapLogging:
    def test_configures_core_and_integration_children(self) -> None:
        stream = io.StringIO()

        bootstrap_logging(
            level="info",
            render_mode="json",
            stream=stream,
            install_uncaught=False,
        )
        Logger("forze.application").info("app")
        Logger("forze.integrations.cache").info("cache")
        Logger("forze_kits.integrations").info("kits")

        events = {r["event"] for r in _json_records(stream)}
        assert {"app", "cache", "kits"} <= events

    def test_extra_and_third_party_names(self) -> None:
        stream = io.StringIO()

        bootstrap_logging(
            level="info",
            render_mode="json",
            logger_names=["myapp"],
            third_party=["third.party"],
            stream=stream,
            install_uncaught=False,
        )
        get_logger("myapp").info("mine")
        logging.getLogger("third.party").warning("foreign")

        text = stream.getvalue()
        assert "mine" in text
        assert "foreign" in text

    def test_dedup_applies_end_to_end(self) -> None:
        stream = io.StringIO()

        bootstrap_logging(
            level="info",
            render_mode="json",
            stream=stream,
            install_uncaught=False,
        )
        for _ in range(10):
            Logger("forze.application").warning(
                "flap", _dedup_key="x", _dedup_window=1000
            )

        assert stream.getvalue().count("flap") == 1
