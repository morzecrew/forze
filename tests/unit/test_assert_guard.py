"""No-``assert`` guard: production code must raise explicitly, never ``assert``.

``assert`` statements are stripped under ``python -O`` (``PYTHONOPTIMIZE``), so any
invariant they guard silently disappears in an optimized deployment. The project rule:
``assert`` belongs in tests only; production code raises an explicit exception
(``exc.internal(...)`` for framework invariants, the appropriate ``exc`` kind for
caller-facing preconditions).

This AST guard fails if any module under ``src/`` or ``examples/`` contains an
``assert`` statement — including multiline forms and ``assert`` with a ``# nosec``
annotation, which a naive grep-based sweep can miss. There is no allowlist: the
tree is clean and must stay clean. (``examples/`` is covered because the recipes
are runnable, doc-included code held to the same bar as ``src/``.)
"""

from __future__ import annotations

import ast
from pathlib import Path

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCANNED_ROOTS = (_REPO / "src", _REPO / "examples")


def _iter_source_files() -> list[Path]:
    return sorted(
        p
        for root in _SCANNED_ROOTS
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts
    )


def _rel(path: Path) -> str:
    try:
        return path.relative_to(_REPO).as_posix()
    except ValueError:
        return path.name  # a file outside the repo (e.g. a guard self-test fixture)


def _assert_sites_in(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [
        f"{_rel(path)}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Assert)
    ]


# ....................... #


def test_no_assert_statements_in_src_or_examples() -> None:
    violations: list[str] = []
    for path in _iter_source_files():
        violations.extend(_assert_sites_in(path))

    assert not violations, (
        "``assert`` statements are stripped under ``python -O``, so production code must "
        "raise explicitly instead (exc.internal(...) for framework invariants, the proper "
        "exc kind for caller-facing preconditions; asserts belong in tests only). "
        "Offending sites:\n  " + "\n  ".join(violations)
    )


def test_guard_flags_assert_statements(tmp_path: Path) -> None:
    # The check must actually fire (not merely pass because src happens to be clean):
    # synthetic assert sites are detected — including the multiline and nosec-annotated
    # forms a grep-based sweep misses — and an explicit raise is not.
    offender = tmp_path / "offender.py"
    offender.write_text(
        "def f(x, y):\n"
        "    assert x  # nosec: B101\n"
        "    assert (\n"
        "        x\n"
        "        and y\n"
        "    ), 'multiline'\n"
    )
    assert len(_assert_sites_in(offender)) == 2

    clean = tmp_path / "clean.py"
    clean.write_text(
        "def f(x):\n"
        "    if not x:\n"
        "        raise RuntimeError('x required')\n"
    )
    assert not _assert_sites_in(clean)
