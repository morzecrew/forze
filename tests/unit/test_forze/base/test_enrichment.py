"""Tests for exception detail enrichment."""

from __future__ import annotations

from forze.base.exceptions import exc
from forze.base.exceptions.enrichment import (
    CallsiteFrame,
    enrich,
    pick_semantic_details,
)

# ----------------------- #


class TestPickSemanticDetails:
    def test_skips_reserved_keys(self) -> None:
        out = pick_semantic_details(
            {
                "self": "ignored",
                "cls": "ignored",
                "callsite": "ignored",
                "user_id": "u1",
            },
        )
        assert "self" not in out
        assert out["user_id"] == "u1"


class TestEnrich:
    def test_adds_callsite_resource_and_semantic(self) -> None:
        err = exc.domain("failed", details={"existing": True})
        frame = CallsiteFrame(domain="document", site="query", route="projects")

        enriched = enrich(
            err,
            callsite=frame,
            resource={"id": "1"},
            cause={"type": "TimeoutError"},
            user_id="u1",
        )

        assert enriched is not err
        assert enriched.details is not None
        assert enriched.details["existing"] is True
        assert enriched.details["callsite"]["domain"] == "document"
        assert enriched.details["resource"]["id"] == "1"
        assert enriched.details["cause"]["type"] == "TimeoutError"
        assert enriched.details["user_id"] == "u1"

    def test_does_not_overwrite_existing_reserved_keys(self) -> None:
        err = exc.domain("failed", details={"callsite": {"domain": "keep"}})
        enriched = enrich(err, callsite=CallsiteFrame(domain="new", site="x"))
        assert enriched.details is not None
        assert enriched.details["callsite"]["domain"] == "keep"
