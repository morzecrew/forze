"""Metric-catalog parity: the docs, the dashboards and the alert pack track `src/`.

Shipped observability assets that silently rot are worse than none — a dashboard whose
panels quietly stopped matching anything reads as "nothing is happening" rather than as a
broken query, and an alert rule on a renamed metric is an alert that never fires again.

So the metric names in `src/` are the single source of truth, collected here by walking
the AST (no imports, so an optional integration that is not installed still counts), and
three things are checked against them:

1. the reference table in `pages/docs/reference/metrics.md`, in **both** directions;
2. every metric referenced by a shipped dashboard or alert rule exists;
3. the millisecond histogram ladder in `forze.base.telemetry` — which has to spell its
   two instrument names as literals, because `forze.base` may not import the layers that
   declare them — still names real metrics.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Final

import pytest
import yaml

from forze.base.telemetry import MILLISECOND_HISTOGRAM_INSTRUMENTS

# ----------------------- #

_REPO: Final[Path] = Path(__file__).resolve().parents[2]
_SRC: Final[Path] = _REPO / "src"
_CATALOG: Final[Path] = _REPO / "pages" / "docs" / "reference" / "metrics.md"
_ASSETS: Final[Path] = _REPO / "pages" / "docs" / "running-in-prod" / "assets" / "grafana"
_DASHBOARDS: Final[Path] = _ASSETS / "dashboards"

_INSTRUMENT_SUFFIXES: Final[tuple[str, ...]] = ("_COUNTER", "_GAUGE", "_HISTOGRAM")
"""Every metric name in `src/` is a module-level constant ending in one of these."""

_HISTOGRAM_SERIES_SUFFIXES: Final[tuple[str, ...]] = ("_bucket", "_sum", "_count")
"""What Prometheus expands a histogram into, and what a p95 query actually selects."""


# ....................... #


def _collect_metric_names() -> dict[str, str]:
    """Map every declared metric name to the module that declares it."""

    found: dict[str, str] = {}

    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

        for node in tree.body:
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
                continue

            value = node.value.value

            if not isinstance(value, str) or not value.startswith("forze."):
                continue

            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue

                if target.id.endswith(_INSTRUMENT_SUFFIXES):
                    found[value] = str(path.relative_to(_REPO))

    return found


METRICS: Final[dict[str, str]] = _collect_metric_names()

PROMETHEUS_NAMES: Final[dict[str, str]] = {
    name.replace(".", "_"): name for name in METRICS
}
"""The Prometheus spelling the shipped assets use: dots to underscores, no unit or
``_total`` suffixes, which is what ``add_metric_suffixes = false`` in the Alloy config
produces. The recipe documents the choice; this is what makes it checkable."""


# ....................... #


def _documented_metric_names() -> list[str]:
    """First-column backticked names from every table row in the catalog page."""

    names: list[str] = []

    for line in _CATALOG.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()

        if not stripped.startswith("|"):
            continue

        first_cell = stripped.split("|")[1].strip()
        match = re.fullmatch(r"`(forze\.[a-z0-9_.]+)`", first_cell)

        if match:
            names.append(match.group(1))

    return names


# ....................... #

_LABEL_BLOCK = re.compile(r"\{[^{}]*\}")
_GROUPING = re.compile(r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^()]*\)")
_METRIC_TOKEN = re.compile(r"\bforze_[a-z0-9_]+\b")


def _referenced_metrics(expression: str) -> set[str]:
    """Metric names an expression selects, with label selectors and groupings stripped.

    Label *names* share the ``forze_`` prefix with metric names (``forze_outcome``,
    ``forze_policy``), so they have to be removed before matching or every panel would
    look like it referenced a metric that does not exist.
    """

    text = expression

    while True:
        collapsed = _LABEL_BLOCK.sub(" ", text)

        if collapsed == text:
            break

        text = collapsed

    text = _GROUPING.sub(" ", text)

    return set(_METRIC_TOKEN.findall(text))


def _resolve(token: str) -> str | None:
    """The declared metric a referenced series belongs to, or ``None`` if unknown."""

    if token in PROMETHEUS_NAMES:
        return PROMETHEUS_NAMES[token]

    for suffix in _HISTOGRAM_SERIES_SUFFIXES:
        base = token.removesuffix(suffix)

        if base != token and base in PROMETHEUS_NAMES:
            return PROMETHEUS_NAMES[base]

    return None


# ....................... #


def _dashboard_expressions() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for path in sorted(_DASHBOARDS.glob("*.json")):
        board = json.loads(path.read_text(encoding="utf-8"))

        for panel in board["panels"]:
            for query in panel.get("targets", ()):
                out.append((f"{path.name}:{panel['title']}", query["expr"]))

    return out


def _alert_rules() -> list[tuple[str, dict[str, object]]]:
    document = yaml.safe_load((_ASSETS / "alerts.yml").read_text(encoding="utf-8"))

    return [
        (group["name"], rule) for group in document["groups"] for rule in group["rules"]
    ]


# ----------------------- #


class TestCollection:
    def test_the_collector_finds_the_known_planes(self) -> None:
        """A collector that silently found nothing would make every check below vacuous."""

        assert "forze.operations" in METRICS
        assert "forze.resilience.events" in METRICS
        assert "forze.crypto.cold_miss" in METRICS
        assert "forze.jobs.stalled" in METRICS
        # Integration packages count too — they are walked, not imported.
        assert "forze.realtime.gateway.emitted" in METRICS
        assert "forze.authn.tokens.signed" in METRICS
        assert len(METRICS) >= 40


# ----------------------- #


class TestCatalogPage:
    def test_every_metric_is_documented(self) -> None:
        undocumented = {
            name: module for name, module in METRICS.items() if name not in _documented_metric_names()
        }

        assert not undocumented, (
            f"metrics exist in src/ but not in {_CATALOG.relative_to(_REPO)}: {undocumented}"
        )

    # ....................... #

    def test_every_documented_metric_still_exists(self) -> None:
        stale = [name for name in _documented_metric_names() if name not in METRICS]

        assert not stale, (
            f"{_CATALOG.relative_to(_REPO)} documents metrics that no longer exist: {stale}"
        )

    # ....................... #

    def test_no_metric_is_listed_twice(self) -> None:
        documented = _documented_metric_names()

        assert len(documented) == len(set(documented))


# ----------------------- #


class TestShippedAssets:
    def test_dashboards_are_loadable_and_identified(self) -> None:
        boards = sorted(path.name for path in _DASHBOARDS.glob("*.json"))

        assert boards == [
            "forze-data-planes.json",
            "forze-operations.json",
            "forze-realtime.json",
            "forze-resilience.json",
        ]

        for path in _DASHBOARDS.glob("*.json"):
            board = json.loads(path.read_text(encoding="utf-8"))

            assert board["uid"] == path.stem
            assert board["panels"]

    # ....................... #

    @pytest.mark.parametrize(("where", "expression"), _dashboard_expressions())
    def test_dashboard_panels_reference_real_metrics(self, where: str, expression: str) -> None:
        unknown = {
            token for token in _referenced_metrics(expression) if _resolve(token) is None
        }

        assert not unknown, f"{where} queries metrics that do not exist: {sorted(unknown)}"

    # ....................... #

    @pytest.mark.parametrize(("group", "rule"), _alert_rules())
    def test_alert_rules_reference_real_metrics(
        self,
        group: str,
        rule: dict[str, object],
    ) -> None:
        unknown = {
            token for token in _referenced_metrics(str(rule["expr"])) if _resolve(token) is None
        }

        assert not unknown, (
            f"{group}/{rule['alert']} fires on metrics that do not exist: {sorted(unknown)}"
        )

    # ....................... #

    @pytest.mark.parametrize(("group", "rule"), _alert_rules())
    def test_every_alert_explains_itself(self, group: str, rule: dict[str, object]) -> None:
        """The point of the pack: the docstring's reasoning travels with the rule."""

        annotations = rule["annotations"]

        assert isinstance(annotations, dict)
        assert annotations.get("summary")
        assert len(str(annotations.get("description", ""))) > 80, (
            f"{group}/{rule['alert']} has no usable description — whoever gets paged needs "
            f"to know what it means and what to do"
        )


# ----------------------- #


class TestBootstrapViewTargets:
    def test_the_millisecond_ladder_names_real_histograms(self) -> None:
        """``forze.base`` cannot import the layers that declare these, so it repeats them.

        That duplication is the whole reason this assertion exists: rename either
        histogram and the ladder would silently stop being installed, leaving the two
        instruments the framework records in milliseconds on second-oriented buckets.
        """

        missing = [name for name in MILLISECOND_HISTOGRAM_INSTRUMENTS if name not in METRICS]

        assert not missing, f"bootstrap_telemetry installs views for unknown metrics: {missing}"
