"""Root pytest configuration for the Forze test suite.

Beyond the usual plumbing this file carries the **conformance census**: an opt-in
collection hook that records which ``(plane, engine)`` differential legs the suite
actually contains. It exists because the conformance ratchet
(``.github/scripts/conformance_manifest.py``) must read the legs from collection
rather than from a promise — a manifest that says a leg exists is worth nothing if
nothing checks that pytest can still see it.

The hook is inert unless ``--conformance-census PATH`` is passed, so a normal run
pays nothing for it.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pytest

# ----------------------- #

_CENSUS_OPTION = "--conformance-census"
_MARKER = "conformance"


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        _CENSUS_OPTION,
        default=None,
        metavar="PATH",
        help=(
            "Write the collected conformance-leg census to PATH as JSON and keep going. "
            "Read by .github/scripts/conformance_manifest.py to verify every manifested "
            "(plane, engine) leg is still collectable."
        ),
    )


def pytest_collection_modifyitems(
    config: Any,
    items: list[pytest.Item],
) -> None:
    """Record the conformance legs this collection contains, when asked to.

    Two things are written. The ``legs`` map is the census proper: every
    ``@pytest.mark.conformance(plane=…, engine=…)`` pair pytest can see, with the node
    ids carrying it — that is what proves a manifested leg still exists. ``node_ids``
    is every collected test, both as collected and with its parameter id stripped, so
    the divergence catalog's ``probe=`` links resolve against real tests instead of being
    trusted — whether a row pins a whole battery or one named check inside it.
    """

    destination = config.getoption("conformance_census")

    if destination is None:
        return

    legs: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    malformed: list[str] = []

    for item in items:
        for marker in item.iter_markers(name=_MARKER):
            plane = marker.kwargs.get("plane")
            engine = marker.kwargs.get("engine")

            if not isinstance(plane, str) or not isinstance(engine, str):
                malformed.append(item.nodeid)
                continue

            legs[(plane, engine)].add(_base_node_id(item.nodeid))

    census = {
        "legs": [
            {"plane": plane, "engine": engine, "node_ids": sorted(node_ids)}
            for (plane, engine), node_ids in sorted(legs.items())
        ],
        "malformed": sorted(malformed),
        "node_ids": sorted(
            {item.nodeid for item in items} | {_base_node_id(item.nodeid) for item in items}
        ),
    }

    Path(destination).write_text(json.dumps(census, indent=2) + "\n", encoding="utf-8")


def _base_node_id(node_id: str) -> str:
    """A node id with its parametrisation stripped, so probes survive a new param."""

    head, _, _ = node_id.partition("[")

    return head
