"""Invocation-surface guard: every framework call site that mints metadata names its boundary.

``InvocationMetadata.surface`` and ``mint_invocation_metadata(surface=)`` default to
``unknown`` so no constructor breaks (RFC 0079 decision 5). The price of that default is that a
boundary which forgets to pass its value compiles, runs, and records ``unknown`` on every log
line and span it produces. A behaviour test only covers the boundaries somebody wrote one for;
this structural check covers every call site in ``src/``, including the next one.

Each site must pass its own boundary's ``InvocationSurface`` member as a literal, never a value
computed at run time (decision 1). An exemption names why the site is not a boundary.
"""

from __future__ import annotations

import ast
from pathlib import Path

# ----------------------- #

_SRC = Path(__file__).resolve().parents[2] / "src"

_MINTERS: frozenset[str] = frozenset({"mint_invocation_metadata", "InvocationMetadata"})

_BOUNDARIES: dict[str, str] = {
    "forze_fastapi/middlewares/invocation.py": "HTTP",
    "forze_fastapi/realtime/ws.py": "WEBSOCKET",
    "forze_socketio/routing.py": "SOCKETIO",
    "forze_mcp/dispatch.py": "MCP",
    "forze_cli/ops/call.py": "CLI",
    "forze_kits/integrations/inbox/consumer.py": "INBOX",
    "forze_temporal/interceptors/codecs.py": "TEMPORAL",
    "forze_inngest/execution/registration.py": "INNGEST",
    "forze_kits/integrations/durable/runner.py": "DURABLE",
}
"""Path → the ``InvocationSurface`` member every minting call in it must pass."""

_EXEMPT: dict[str, str] = {
    # The minting function itself forwards its caller's ``surface`` argument.
    "forze/application/execution/ingress/invoke.py": "mint_invocation_metadata's own body",
    # Decodes the producer's ids from the event envelope for ``_bind_invocation`` to read as
    # causation and correlation; the decoded value is never bound.
    "forze_inngest/adapters/context.py": "envelope decode, never bound",
}
"""Path → why its construction of metadata is not a boundary minting one."""


# ....................... #


def _minting_calls() -> dict[str, list[ast.Call]]:
    found: dict[str, list[ast.Call]] = {}

    for path in sorted(_SRC.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue

        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue

            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)

            if name in _MINTERS:
                found.setdefault(path.relative_to(_SRC).as_posix(), []).append(node)

    return found


def _surface_member(call: ast.Call) -> str | None:
    """The ``InvocationSurface`` member passed as ``surface=``, or ``None`` if it is not one."""

    for keyword in call.keywords:
        value = keyword.value

        if (
            keyword.arg == "surface"
            and isinstance(value, ast.Attribute)
            and isinstance(value.value, ast.Name)
            and value.value.id == "InvocationSurface"
        ):
            return value.attr

    return None


# ....................... #


def test_every_framework_call_site_passes_its_own_surface() -> None:
    wrong: list[str] = []

    for path, calls in _minting_calls().items():
        if path in _EXEMPT:
            continue

        expected = _BOUNDARIES.get(path)

        for call in calls:
            passed = _surface_member(call)

            if expected is None or passed != expected:
                wrong.append(
                    f"{path}:{call.lineno} passes {passed or 'no InvocationSurface member'}, "
                    f"expected {expected or 'a boundary entry in this guard'}"
                )

    assert not wrong, "\n".join(wrong)


def test_every_boundary_still_mints_metadata() -> None:
    found = _minting_calls()

    # A boundary that stops minting would otherwise pass the check above vacuously.
    assert set(_BOUNDARIES) <= set(found), set(_BOUNDARIES) - set(found)
    assert set(_EXEMPT) <= set(found), set(_EXEMPT) - set(found)
