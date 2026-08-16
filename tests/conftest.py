"""Root pytest configuration for the Forze test suite.

Beyond the usual plumbing this file carries the **conformance census**, in two forms that
answer two different questions about the differential legs:

- ``--conformance-census PATH`` records what pytest can *collect*. That is what proves a
  manifested leg still exists, and it runs offline in ``just quality``.
- ``--conformance-executed PATH`` records what actually *ran*, per leg, with outcomes. A
  collectable leg that skips wholesale — the engine's container never came up, an extra is
  missing, the suite's directory is not in CI's matrix at all — is indistinguishable from a
  passing one in a green build. This closes that: each CI shard writes its own file and the
  coverage job unions them, so a leg nobody ran fails the build.

Both hooks are inert unless their option is passed, so a normal run pays nothing for them.
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
_EXECUTED_OPTION = "--conformance-executed"
_MARKER = "conformance"

_LEG_BY_NODE_ID: dict[str, tuple[str, str]] = {}
"""Node id → its ``(plane, engine)``, filled at collection and read as tests report."""

_OUTCOMES: defaultdict[tuple[str, str], defaultdict[str, int]] = defaultdict(
    lambda: defaultdict(int)
)
"""``(plane, engine)`` → outcome → count, accumulated across the session."""


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
    parser.addoption(
        _EXECUTED_OPTION,
        default=None,
        metavar="PATH",
        help=(
            "Write per-leg execution outcomes to PATH as JSON at the end of the session. "
            "Shard files are unioned by `conformance_manifest.py --executed` to prove every "
            "manifested leg actually ran somewhere, rather than skipping quietly."
        ),
    )


def pytest_configure(config: Any) -> None:
    """Turn off FastMCP's camelCase compatibility bridge for the whole session.

    MCP SDK v2 renamed every protocol field from camelCase (``inputSchema``) to snake_case
    (``input_schema``), and FastMCP bridges the old spellings with warn-once properties.
    That bridge is a migration aid on its way out — a suite that leans on it passes today
    and breaks when the shim is dropped, and the only signal in between is a
    ``DeprecationWarning`` this repository's ``filterwarnings`` silences.

    Switching it off makes an old spelling an ``AttributeError`` at the line that reads it,
    which is what keeps the migration done rather than merely finished once.
    """

    del config

    try:
        import fastmcp
    except ImportError:  # the `mcp` extra is optional; nothing to gate without it
        return

    # A FastMCP that no longer carries the setting no longer carries the bridge either, so
    # the old spellings already fail on their own and there is nothing left to switch off.
    if hasattr(fastmcp.settings, "mcp_camelcase_compat"):
        fastmcp.settings.mcp_camelcase_compat = False


def pytest_collection_modifyitems(
    config: Any,
    items: list[pytest.Item],
) -> None:
    """Read the conformance markers off this collection, for either census.

    The collected census writes two things. The ``legs`` map is the census proper: every
    ``@pytest.mark.conformance(plane=…, engine=…)`` pair pytest can see, with the node ids
    carrying it. ``node_ids`` is every collected test, both as collected and with its
    parameter id stripped, so the divergence catalog's ``probe=`` links resolve against real
    tests instead of being trusted — whether a row pins a whole battery or one named check
    inside it.
    """

    destination = config.getoption("conformance_census")
    executed = config.getoption("conformance_executed")

    if destination is None and executed is None:
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
            _LEG_BY_NODE_ID[item.nodeid] = (plane, engine)

    if destination is None:
        return

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


def pytest_runtest_logreport(report: Any) -> None:
    """Tally each leg's outcomes.

    A skip decided in a fixture never reaches the ``call`` phase, so setup reports count
    too — that is precisely the case worth catching, since a leg whose container never
    started skips at setup and reports nothing at all otherwise.
    """

    leg = _LEG_BY_NODE_ID.get(report.nodeid)

    if leg is None:
        return

    if report.when == "call" or (report.when == "setup" and report.outcome != "passed"):
        _OUTCOMES[leg][report.outcome] += 1


def pytest_sessionfinish(session: Any) -> None:
    destination = session.config.getoption("conformance_executed")

    if destination is None:
        return

    path = Path(destination)
    # Under xdist every worker finishes its own session; give each a distinct file rather
    # than letting them overwrite one another. The combiner unions whatever it is given.
    worker = getattr(session.config, "workerinput", {}).get("workerid")

    if worker:
        path = path.with_name(f"{path.stem}-{worker}{path.suffix}")

    payload = {
        "legs": [
            {"plane": plane, "engine": engine, **dict(sorted(outcomes.items()))}
            for (plane, engine), outcomes in sorted(_OUTCOMES.items())
        ],
    }

    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _base_node_id(node_id: str) -> str:
    """A node id with its parametrisation stripped, so probes survive a new param."""

    head, _, _ = node_id.partition("[")

    return head
