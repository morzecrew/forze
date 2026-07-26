"""Counterparty-rotated credential value objects: leak safety and expiry semantics."""

from __future__ import annotations

from datetime import timedelta

from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    ExchangedCredential,
    RotatingCredential,
    SecretVersion,
)
from forze.base.primitives import utcnow

# ----------------------- #


class TestLeakSafety:
    def test_no_token_appears_in_a_repr(self) -> None:
        """These objects ride in exception context and structured logs, so a token in
        ``repr`` is a token in the log aggregator."""

        exchanged = ExchangedCredential(
            access_token="super-secret-access",
            refresh_token="super-secret-refresh",
            metadata={"host": "acme.example"},
        )
        stored = RotatingCredential(
            access_token="super-secret-access",
            version=SecretVersion("7"),
            metadata={"host": "acme.example"},
        )

        for rendered in (repr(exchanged), repr(stored)):
            assert "super-secret-access" not in rendered
            assert "super-secret-refresh" not in rendered
            # Metadata is not secret and stays visible — it is what makes a log useful.
            assert "acme.example" in rendered

    def test_the_caller_facing_view_has_no_refresh_token_at_all(self) -> None:
        """Structural, not conventional: a caller cannot replay a rotated token because
        the type it holds has nowhere to put one."""

        stored = RotatingCredential(access_token="a", version=SecretVersion("1"))

        assert not hasattr(stored, "refresh_token")
        assert "refresh_token" in {field.name for field in ExchangedCredential.__attrs_attrs__}


class TestExpiry:
    def test_a_credential_without_a_stated_expiry_never_reports_itself_spent(self) -> None:
        """The counterparty did not say when it dies, so only a rejected call can prove it —
        guessing would rotate healthy credentials on a schedule nobody agreed to."""

        stored = RotatingCredential(access_token="a", version=SecretVersion("1"))

        assert not stored.expires_before(utcnow() + timedelta(days=365))

    def test_expiry_is_inclusive_and_supports_a_caller_skew(self) -> None:
        now = utcnow()
        stored = RotatingCredential(
            access_token="a",
            version=SecretVersion("1"),
            expires_at=now,
        )

        assert stored.expires_before(now)
        assert not stored.expires_before(now - timedelta(seconds=1))

        # A caller wanting margin asks about the future rather than configuring the VO.
        soon = RotatingCredential(
            access_token="a",
            version=SecretVersion("1"),
            expires_at=now + timedelta(seconds=20),
        )

        assert soon.expires_before(now + timedelta(seconds=30))
        assert not soon.expires_before(now)


class TestCodes:
    def test_every_code_is_distinct(self) -> None:
        """Callers branch on these, so a collision would merge a retryable outcome with a
        terminal one."""

        codes = (
            BURNT_CREDENTIAL_CODE,
            CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
            CREDENTIAL_PERSIST_LOST_CODE,
            INVALID_GRANT_CODE,
        )

        assert len(set(codes)) == len(codes)
        assert all(code == code.lower().strip() for code in codes)
