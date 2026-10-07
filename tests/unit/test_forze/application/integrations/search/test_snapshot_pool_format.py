"""Every snapshot fingerprint carries the pool format, so a run written in an older one cannot
replay after the format changes."""

from __future__ import annotations

import pytest

from forze.application.integrations.search import snapshot
from forze.application.integrations.search.snapshot import SearchResultSnapshot


def _fingerprints() -> tuple[str, str, str]:
    return (
        SearchResultSnapshot.simple_search_fingerprint("q", None, None, spec_name="s", variant="v"),
        SearchResultSnapshot.hub_search_fingerprint(
            "q",
            None,
            None,
            spec_name="h",
            members_weighted=[("m", 1.0)],
            score_merge="max",
            combine="union",
        ),
        SearchResultSnapshot.federated_fingerprint("q", None, None, spec_name="f"),
    )


def test_every_fingerprint_moves_with_the_pool_format(monkeypatch: pytest.MonkeyPatch) -> None:
    before = _fingerprints()
    monkeypatch.setattr(snapshot, "_POOL_FORMAT", snapshot._POOL_FORMAT + 1)  # pyright: ignore[reportPrivateUsage]
    after = _fingerprints()

    assert all(b != a for b, a in zip(before, after, strict=True))
