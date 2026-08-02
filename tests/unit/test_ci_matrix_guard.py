"""CI matrix guard: every integration suite is actually run by CI, or is triaged here.

A test directory that no CI job names is a test directory that never runs. Nothing about a
green build says otherwise — the suite is not failing, it is absent, and absence is exactly
what a passing pipeline looks like. That is not hypothetical: ``test_forze_inference`` and
``test_portability`` sat outside the matrix, so two of the four inference conformance legs
and the whole portability round-trip had never run in CI at all. They were found by the
conformance execution gate *after* a full run; this guard is the same fact checked
statically, in ``just quality``, seconds after someone adds a directory.

The companion gate (``.github/scripts/conformance_manifest.py --executed``) still matters
and is not replaced by this: it catches a suite that IS in the matrix but skipped at
runtime. This one catches a suite that was never wired up at all.

An exemption is a claim that the suite is deliberately not part of CI — never that someone
has not gotten around to it.
"""

from __future__ import annotations

from pathlib import Path

import yaml

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_WORKFLOW = _REPO / ".github" / "workflows" / "ci.yml"
_INTEGRATION = _REPO / "tests" / "integration"

_EXEMPTIONS: dict[str, str] = {
    "test_forze_identity_live": (
        "Live smoke tests against VK ID's real public_info endpoint, gated behind "
        "FORZE_LIVE_IDP_TESTS=1. They hit a third-party vendor over the network, so running "
        "them on every push would make CI depend on someone else's uptime and rate limits."
    ),
}
"""Integration suites intentionally outside CI, each with the reason it stays out."""


def _matrix_paths() -> set[str]:
    """Every `tests/...` path the CI test matrix names."""

    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    included = workflow["jobs"]["test"]["strategy"]["matrix"]["include"]

    return {str(entry["path"]) for entry in included}


def _integration_suites() -> set[str]:
    return {
        entry.name
        for entry in _INTEGRATION.iterdir()
        if entry.is_dir() and not entry.name.startswith("__")
    }


# ....................... #


def test_every_integration_suite_is_in_the_ci_matrix_or_triaged() -> None:
    covered = {Path(path).name for path in _matrix_paths()}
    unrun = sorted(_integration_suites() - covered - set(_EXEMPTIONS))

    assert not unrun, (
        "Integration suite(s) no CI job runs: "
        + ", ".join(unrun)
        + ". Add a matrix entry in .github/workflows/ci.yml, or add an entry to "
        "_EXEMPTIONS naming why the suite is deliberately not run there."
    )


def test_no_matrix_entry_points_at_a_missing_directory() -> None:
    """The reverse direction: a shard whose path was renamed away runs nothing, silently."""

    missing = sorted(path for path in _matrix_paths() if not (_REPO / path).is_dir())

    assert not missing, f"CI matrix names path(s) that do not exist: {', '.join(missing)}"


def test_exemptions_are_not_stale_and_carry_a_reason() -> None:
    suites = _integration_suites()

    for name, reason in _EXEMPTIONS.items():
        assert name in suites, f"exemption for {name!r} names a suite that no longer exists"
        assert len(reason) > 60, f"{name}: give a real reason, not a label"

    covered = {Path(path).name for path in _matrix_paths()}
    contradicted = sorted(set(_EXEMPTIONS) & covered)

    assert not contradicted, (
        "Exemption(s) for suite(s) CI now runs — delete them so the list cannot rot into a "
        "place where a genuinely unrun suite hides: " + ", ".join(contradicted)
    )


def test_the_guard_detects_an_unrun_suite() -> None:
    """The guard must be able to fail, or it passes by checking nothing."""

    covered = {Path(path).name for path in _matrix_paths()}
    suites = {*_integration_suites(), "test_forze_newly_added"}

    assert sorted(suites - covered - set(_EXEMPTIONS)) == ["test_forze_newly_added"]
