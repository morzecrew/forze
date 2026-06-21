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
  ``datetime.now``/``datetime.utcnow`` (use ``utcnow()`` / ``monotonic()`` instead);
* stable bucketing — ``hash(x) % n`` / ``hash(x) & mask``: Python's builtin ``hash``
  is PYTHONHASHSEED-randomized, so it picks a different bucket every process and breaks
  cross-process replay. Use a stable hash (``zlib.crc32`` / ``derive_seed``).

Type annotations and bare references (e.g. ``rng: random.Random``,
``clock=time.monotonic`` as a *type*) are not flagged — only calls. ``time.perf_counter``
is intentionally allowed: pure elapsed measurement for observability, which never feeds
program logic or output and is legitimately real even under simulation.

A second, **scoped** check (``test_no_raw_thread_offload_in_application_layer``) bans raw
``asyncio.to_thread`` / ``loop.run_in_executor`` in the layers that run under simulation
(``forze.application``, ``forze.base``, ``forze.domain``, ``forze_kits``) — they raise
``RealIOForbidden`` under the simulator, so the code can't be DST-tested. Offload through
``run_cpu`` / ``run_cpu_map`` instead. Integration adapters are *not* scanned: they are
replaced by in-memory mocks under DST, so their real executors never run in a simulation.
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
        "forze_dst/cluster.py",
        "forze_dst/scheduler.py",
        # The coverage-guided fuzzer builds its mutation-lineage RNG from the master seed.
        "forze_dst/explore_guided.py",
        # The run substrate + engines build their seeded fault/input/crash RNGs from sub-seeds.
        "forze_dst/engines/context.py",
        "forze_dst/engines/scenario.py",
        "forze_dst/engines/crash_restart.py",
        "forze_dst/engines/guided.py",
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
        {
            "monotonic",
            "monotonic_ns",
            "time",
            "time_ns",
            "process_time",
            "process_time_ns",
        }
    ),
    "datetime": frozenset({"now", "utcnow"}),
}

# Names that, when imported via ``from <module> import <name>``, become banned
# bare-name calls.
_BANNED_FROM_IMPORT: dict[str, frozenset[str]] = {
    "os": frozenset({"urandom"}),
    "secrets": _BANNED_ATTRS["secrets"],  # type: ignore[dict-item]
    "random": frozenset(
        {
            "random",
            "uniform",
            "randint",
            "randrange",
            "choice",
            "shuffle",
            "getrandbits",
            "Random",
            "SystemRandom",
            "sample",
            "betavariate",
        }
    ),
    "uuid": frozenset({"uuid4", "uuid1"}),
    "time": frozenset(
        {
            "monotonic",
            "monotonic_ns",
            "time",
            "time_ns",
            "process_time",
            "process_time_ns",
        }
    ),
}


# ....................... #


def _iter_source_files() -> list[Path]:
    return sorted(p for p in _SRC.rglob("*.py") if "__pycache__" not in p.parts)


def _rel(path: Path) -> str:
    try:
        return path.relative_to(_SRC).as_posix()
    except ValueError:
        return path.name  # a file outside src/ (e.g. a guard self-test fixture)


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
        # ``hash(x) % n`` / ``hash(x) & mask`` — PYTHONHASHSEED-randomized bucketing.
        if (
            isinstance(node, ast.BinOp)
            and isinstance(node.op, (ast.Mod, ast.BitAnd))
            and isinstance(node.left, ast.Call)
            and isinstance(node.left.func, ast.Name)
            and node.left.func.id == "hash"
        ):
            found.append(
                f"{_rel(path)}:{node.lineno}: hash(...) % / & "
                "(use zlib.crc32 / derive_seed for cross-process-stable bucketing)"
            )

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
        "Non-deterministic primitives must route through the seams: entropy via "
        "forze.base.primitives.current_entropy_source / token_urlsafe / uuid4, time via "
        "utcnow() / monotonic(), and stable bucketing via zlib.crc32 / derive_seed (not "
        "Python's hash()). Offending sites:\n  " + "\n  ".join(violations)
    )


# ----------------------- #
# Thread-offload guard (scoped): handler/application code must offload via run_cpu,
# not raw asyncio.to_thread / loop.run_in_executor — the latter raise RealIOForbidden
# under simulation, so the code can't be DST-tested. Integration adapters are exempt:
# they are replaced by in-memory mocks under DST, so their real executors never run in
# a simulation.

_OFFLOAD_GUARDED_PREFIXES: tuple[str, ...] = (
    "forze/application/",
    "forze/base/",
    "forze/domain/",
    "forze_kits/",
)

# The CPU-offload seam itself owns the one legitimate run_in_executor.
_OFFLOAD_ALLOWLIST: frozenset[str] = frozenset({"forze/base/primitives/cpu.py"})


def _offload_violations_in(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    # ``from asyncio import to_thread`` → a banned bare ``to_thread(...)`` call.
    bare_offload: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "asyncio":
            for alias in node.names:
                if alias.name == "to_thread":
                    bare_offload.add(alias.asname or alias.name)

    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr == "run_in_executor":
                found.append(f"{_rel(path)}:{node.lineno}: .run_in_executor(...)")
            elif (
                func.attr == "to_thread"
                and isinstance(func.value, ast.Name)
                and func.value.id == "asyncio"
            ):
                found.append(f"{_rel(path)}:{node.lineno}: asyncio.to_thread(...)")
        elif isinstance(func, ast.Name) and func.id in bare_offload:
            found.append(f"{_rel(path)}:{node.lineno}: {func.id}(...)")

    return found


def test_no_raw_thread_offload_in_application_layer() -> None:
    violations: list[str] = []
    for path in _iter_source_files():
        rel = _rel(path)
        if not rel.startswith(_OFFLOAD_GUARDED_PREFIXES) or rel in _OFFLOAD_ALLOWLIST:
            continue
        violations.extend(_offload_violations_in(path))

    assert not violations, (
        "Handler/application code must offload CPU or blocking work through "
        "forze.base.primitives.run_cpu / run_cpu_map, not raw asyncio.to_thread / "
        "loop.run_in_executor — the latter raise RealIOForbidden under simulation, so the "
        "code cannot be DST-tested. Integration adapters are exempt (mocked under DST). "
        "Offending sites:\n  " + "\n  ".join(violations)
    )


def test_offload_guard_flags_raw_to_thread(tmp_path: Path) -> None:
    # The offload check must actually fire: synthetic raw-offload sites are detected,
    # a run_cpu call is not.
    offender = tmp_path / "offender.py"
    offender.write_text(
        "import asyncio\n"
        "async def f(loop, fn):\n"
        "    await asyncio.to_thread(fn)\n"
        "    await loop.run_in_executor(None, fn)\n"
    )
    assert len(_offload_violations_in(offender)) == 2

    clean = tmp_path / "clean.py"
    clean.write_text(
        "from forze.base.primitives import run_cpu\n"
        "async def f(fn):\n"
        "    return await run_cpu(fn)\n"
    )
    assert not _offload_violations_in(clean)


def test_guard_flags_hash_modulo_bucketing(tmp_path: Path) -> None:
    # The guard's hash()-bucketing check must actually fire (not merely pass because
    # src happens to be clean): a synthetic offender is detected, a stable hash isn't.
    offender = tmp_path / "offender.py"
    offender.write_text(
        "def stripe(k, locks):\n    return locks[hash(k) % len(locks)]\n"
    )
    assert _violations_in(offender), "hash(x) % n must be flagged"

    clean = tmp_path / "clean.py"
    clean.write_text(
        "import zlib\n"
        "def stripe(k, locks):\n"
        "    return locks[zlib.crc32(k.encode()) % len(locks)]\n"
    )
    assert not _violations_in(clean), "a stable-hash bucket must not be flagged"
