"""Determinism guard: raw entropy *and clock* primitives must route through the seams.

DST (deterministic simulation testing) requires that every source of non-determinism
flow through an ambient seam (``forze.base.primitives``) — randomness through
``EntropySource`` and time through ``TimeSource`` (``utcnow``/``monotonic``) — so a
bound ``SeededEntropySource`` + simulation clock can make a run byte-identical and
seed-replayable.

This AST guard fails if any module under ``src/`` *calls* a raw primitive outside the
seam's own definition:

* entropy — ``os.urandom``, ``secrets.token_*``/``randbits``/…, ``random.<fn>``,
  stdlib ``uuid.uuid4``/``uuid1``;
* clocks — ``time.monotonic``/``time``/``time_ns``/``process_time*`` and
  ``datetime.now``/``datetime.utcnow`` (use ``utcnow()`` / ``monotonic()`` instead).

Type annotations and bare references (e.g. ``rng: random.Random``,
``clock=time.monotonic`` as a *type*) are not flagged — only calls. ``time.perf_counter``
is intentionally allowed: pure elapsed measurement for observability, which never feeds
program logic or output and is legitimately real even under simulation.
"""

from __future__ import annotations

import ast
from pathlib import Path

# ----------------------- #

_SRC = Path(__file__).resolve().parents[2] / "src"

# Files allowed to use raw primitives: the seams' own definitions.
_ALLOWLIST: frozenset[str] = frozenset(
    {
        "forze/base/primitives/entropy_source.py",
        "forze/base/primitives/time_source.py",
        # The simulation harness *builds* the deterministic substrate: it constructs
        # seeded RNGs (schedule perturbation, workload generation) on purpose.
        "forze_dst/runtime.py",
        "forze_dst/workload.py",
        "forze_dst/harness.py",
        "forze_dst/scheduler.py",
    }
)

# Per-module banned attributes (entropy draws). ``random.*`` bans every call on the
# module; ``secrets`` bans only the stochastic API (``compare_digest`` is allowed).
_BANNED_ATTRS: dict[str, frozenset[str] | None] = {
    "os": frozenset({"urandom"}),
    "secrets": frozenset(
        {
            "token_bytes",
            "token_hex",
            "token_urlsafe",
            "randbits",
            "randbelow",
            "choice",
            "SystemRandom",
        }
    ),
    "random": None,  # None == every attribute call on the module
    "uuid": frozenset({"uuid4", "uuid1"}),
    # Clocks: route through utcnow()/monotonic(). perf_counter* is NOT banned
    # (observability-only elapsed measurement).
    "time": frozenset(
        {"monotonic", "monotonic_ns", "time", "time_ns", "process_time", "process_time_ns"}
    ),
    "datetime": frozenset({"now", "utcnow"}),
}

# Names that, when imported via ``from <module> import <name>``, become banned
# bare-name calls.
_BANNED_FROM_IMPORT: dict[str, frozenset[str]] = {
    "os": frozenset({"urandom"}),
    "secrets": _BANNED_ATTRS["secrets"],  # type: ignore[dict-item]
    "random": frozenset(
        {"random", "uniform", "randint", "randrange", "choice", "shuffle",
         "getrandbits", "Random", "SystemRandom", "sample", "betavariate"}
    ),
    "uuid": frozenset({"uuid4", "uuid1"}),
    "time": frozenset(
        {"monotonic", "monotonic_ns", "time", "time_ns", "process_time", "process_time_ns"}
    ),
}


# ....................... #


def _iter_source_files() -> list[Path]:
    return sorted(
        p
        for p in _SRC.rglob("*.py")
        if "__pycache__" not in p.parts
    )


def _rel(path: Path) -> str:
    return path.relative_to(_SRC).as_posix()


def _violations_in(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    # Bare names bound to a banned primitive via ``from x import y``.
    banned_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _BANNED_FROM_IMPORT:
            allowed = _BANNED_FROM_IMPORT[node.module]
            for alias in node.names:
                if alias.name in allowed:
                    banned_names.add(alias.asname or alias.name)

    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        # module.attr(...) form
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            module, attr = func.value.id, func.attr
            banned = _BANNED_ATTRS.get(module, "missing")
            if banned == "missing":
                continue
            if banned is None or attr in banned:
                found.append(f"{_rel(path)}:{node.lineno}: {module}.{attr}(...)")
        # bare name(...) form, from a banned ``from`` import
        elif isinstance(func, ast.Name) and func.id in banned_names:
            found.append(f"{_rel(path)}:{node.lineno}: {func.id}(...)")

    return found


# ....................... #


def test_no_raw_entropy_outside_seam() -> None:
    violations: list[str] = []
    for path in _iter_source_files():
        if _rel(path) in _ALLOWLIST:
            continue
        violations.extend(_violations_in(path))

    assert not violations, (
        "Raw entropy primitives must route through the EntropySource seam "
        "(forze.base.primitives.current_entropy_source / token_urlsafe / uuid4). "
        "Offending call sites:\n  " + "\n  ".join(violations)
    )
