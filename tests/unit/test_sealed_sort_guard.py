"""Sealed-sort guard: every backend's sort seam must be handed the spec's sealed fields.

A field-encrypted column has no usable order at rest — a randomized ciphertext has no order at
all, a deterministic one preserves equality but not order — so ``ORDER BY`` on one silently
returns ciphertext order, and a keyset cursor additionally carries the field's raw sort value in
its token. The rule is enforced by passing ``sealed=`` to the shared sort seams, which raise
``core.crypto.encrypted_sort_field``.

**This guard exists because the seam is not the fix — the call sites are.** ``sealed`` is threaded
per backend (a gateway holds *derived frozensets*, never the spec), so "the shared validator
refuses it" is only true for a backend someone remembered to wire. That has now failed twice:

1. ``FieldEncryption.forbidden_sort_fields`` was written, tested, and wired for the **search**
   plane only; the document plane never called it, and sorting a document by a sealed field
   returned ciphertext order on real Postgres for the whole life of the feature.
2. The fix for (1) wired **Postgres and the mock**, shipped, and claimed "every backend agrees" —
   while **Mongo and Firestore** silently kept sorting by ciphertext. The tests passed because
   they covered exactly the two backends that had been threaded.

Both were invisible to behaviour tests, because a behaviour test only covers a backend somebody
thought to write one for — the same wiring gap, one level up. A structural check cannot be
forgotten: a new backend, or a new sort call site in an existing one, fails here immediately.

Each allowlist entry names *why* a site is exempt. An exemption is a claim that the rule is
enforced somewhere else — not that it does not apply.
"""

from __future__ import annotations

import ast
from pathlib import Path

# ----------------------- #

_SRC = Path(__file__).resolve().parents[2] / "src"

_SEAMS: frozenset[str] = frozenset(
    {
        "resolve_sort_keys",  # offset ORDER BY / in-memory sort
        "normalize_sorts_for_keyset",  # keyset + cursor token
        "validate_runtime_sort_fields",  # driver-side backends (mongo/firestore/mock)
        "validate_sort_fields",  # read-fields validation (spec construction, keyset)
    }
)
"""The sort seams that accept ``sealed=``. There is deliberately no single one: offset, keyset and
driver-side validation are three different paths, and **Postgres does not call
``validate_runtime_sort_fields`` at all** — so guarding "the" seam is not a thing one can do."""

# Only code that resolves a *spec's* sorts is scanned. The sort_resolution package is where the
# seams are defined and chained to each other, so it is not a caller.
_GUARDED_PREFIXES: tuple[str, ...] = (
    "forze/application/contracts/document/",
    "forze/application/integrations/document/",
    "forze_postgres/",
    "forze_mongo/",
    "forze_firestore/",
    "forze_mock/",
)

_ALLOWLIST: dict[str, str] = {
    # The search plane refuses a sealed sort key at its own seam — reject_encrypted_sort_fields
    # (core.search.encrypted_sort_field), called by the search adapters and offset_executor —
    # so its sort resolution is already covered and does not thread ``sealed`` a second time.
    "forze_postgres/adapters/search/_cursor_run.py": "search: reject_encrypted_sort_fields",
    "forze_postgres/adapters/search/hub/semantics.py": "search: reject_encrypted_sort_fields",
    "forze_mongo/adapters/search/_cursor_run.py": "search: reject_encrypted_sort_fields",
    # The mock's in-memory sort helper takes rows, not a spec; MockDocumentAdapter validates with
    # sealed= before calling it (and the keyset path threads sealed into normalize_sorts_for_keyset).
    "forze_mock/query/matching.py": "mock: adapter validates with sealed= before sorting",
}
"""Path → why the rule is enforced elsewhere for that file."""


# ....................... #


def _iter_source_files() -> list[Path]:
    return sorted(p for p in _SRC.rglob("*.py") if "__pycache__" not in p.parts)


def _rel(path: Path) -> str:
    try:
        return path.relative_to(_SRC).as_posix()
    except ValueError:
        return path.name


def _seam_calls_missing_sealed(path: Path) -> list[str]:
    """Calls to a sort seam that do not pass ``sealed=``."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        # bare ``resolve_sort_keys(...)`` and qualified ``mod.resolve_sort_keys(...)``
        name = (
            func.id
            if isinstance(func, ast.Name)
            else func.attr
            if isinstance(func, ast.Attribute)
            else None
        )

        if name not in _SEAMS:
            continue

        if any(kw.arg == "sealed" for kw in node.keywords):
            continue

        found.append(f"{_rel(path)}:{node.lineno}: {name}(...) without sealed=")

    return found


# ....................... #


def test_every_sort_seam_call_passes_sealed() -> None:
    violations: list[str] = []

    for path in _iter_source_files():
        rel = _rel(path)

        if not rel.startswith(_GUARDED_PREFIXES) or rel in _ALLOWLIST:
            continue

        violations.extend(_seam_calls_missing_sealed(path))

    assert not violations, (
        "A sort seam was called without the spec's sealed fields, so sorting on a "
        "field-encrypted column there orders by ciphertext silently (and a keyset cursor leaks "
        "its raw value in the token). Pass sealed=<FieldEncryption.sealed> — a gateway should "
        "carry it as a `sealed_fields` frozenset, like `lenient_read_fields`. If the rule is "
        "genuinely enforced elsewhere for this file, add it to _ALLOWLIST with the reason. "
        "Offending sites:\n  " + "\n  ".join(violations)
    )


def test_allowlist_entries_still_exist() -> None:
    """An allowlist entry for a file that moved or was deleted is a silent hole — the exemption
    outlives the reason it was granted for."""

    missing = sorted(rel for rel in _ALLOWLIST if not (_SRC / rel).exists())

    assert not missing, (
        "Allowlisted file(s) no longer exist; drop the entry or repoint it: " + ", ".join(missing)
    )


def test_guard_flags_an_unthreaded_seam_call(tmp_path: Path) -> None:
    """The guard must actually fire — otherwise it passes because it checks nothing.

    Mirrors the two real regressions: a backend calling a seam without ``sealed`` (the Mongo /
    Firestore hole), in both the bare-import and qualified-module call forms.
    """

    offender = tmp_path / "offender.py"
    offender.write_text(
        "from forze.application.contracts.querying import resolve_sort_keys\n"
        "import forze.application.contracts.querying as q\n"
        "def render(sorts, gw):\n"
        "    a = resolve_sort_keys(sorts)\n"  # bare call, no sealed
        "    b = q.normalize_sorts_for_keyset(sorts, read_fields=gw.read_fields)\n"  # qualified
        "    return a, b\n"
    )
    assert len(_seam_calls_missing_sealed(offender)) == 2

    clean = tmp_path / "clean.py"
    clean.write_text(
        "from forze.application.contracts.querying import resolve_sort_keys\n"
        "def render(sorts, gw):\n"
        "    return resolve_sort_keys(sorts, sealed=gw.sealed_fields)\n"
    )
    assert not _seam_calls_missing_sealed(clean), "a threaded call must not be flagged"
