"""Reflection gates: property checks over whole code surfaces.

Each helper here checks a *property* rather than a case — every import in a module tree,
every Protocol in a ports module, every operation id on an edge — so a new module, method
or operation is guarded the moment it appears, without anyone writing its test.

A loop over a discovered set passes vacuously when discovery finds nothing, which is how a
gate quietly stops checking anything after a rename. Every gate here therefore refuses an
empty discovery outright: an empty module tree, a ports module with no Protocols, an edge
with no operations each raise instead of passing.

All failures raise :class:`AssertionError` listing every violation, so a gate run in
pytest reads as an ordinary failing test naming everything wrong at once.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import typing
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import FunctionType, ModuleType

# ----------------------- #


def _resolve_module(module: ModuleType | str) -> ModuleType:
    return importlib.import_module(module) if isinstance(module, str) else module


def _fail(header: str, violations: list[str]) -> None:
    lines = "\n".join(f"  - {v}" for v in violations)
    raise AssertionError(f"{header}\n{lines}")


# ----------------------- #
# Purity gate


def _module_files(module: ModuleType) -> list[Path]:
    paths = getattr(module, "__path__", None)

    if paths is not None:
        return sorted(p for base in paths for p in Path(base).rglob("*.py"))

    file = getattr(module, "__file__", None)

    if file is None:
        raise AssertionError(f"Module {module.__name__!r} has no source file to parse")

    return [Path(file)]


def _import_roots(tree: ast.AST) -> list[tuple[int, str]]:
    """Top-level roots of every absolute import, with line numbers; relative imports are
    internal to the package and carry no root."""

    roots: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.extend((node.lineno, alias.name.partition(".")[0]) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.append((node.lineno, node.module.partition(".")[0]))

    return roots


def assert_pure_module(
    module: ModuleType | str,
    *,
    allowed: Iterable[str],
    forbidden: Iterable[str] = (),
) -> None:
    """Assert every absolute import in ``module`` (a module or whole package) is allowed.

    Two independent layers, redundantly on purpose. The **allowlist** catches the module
    nobody thought to name: any import whose top-level root is not in ``allowed`` fails.
    The **forbidden list** is a named refusal checked first and regardless of the
    allowlist, so widening ``allowed`` can never quietly re-admit ``time``, ``random`` or
    whatever else was refused by name.

    The module's own top-level package is implicitly allowed (importing yourself is not
    impurity), and relative imports are internal by construction. A name in both lists is
    a contradiction and fails outright, as does a module tree with no source files.
    """

    mod = _resolve_module(module)
    allow = set(allowed)
    forbid = set(forbidden)

    contradictions = sorted(allow & forbid)

    if contradictions:
        raise AssertionError(
            f"Purity gate for {mod.__name__!r} allows and forbids the same roots: {contradictions}",
        )

    # Importing yourself is not impurity, and ``from __future__ import ...`` is a
    # compiler directive, not a dependency.
    allow.add(mod.__name__.partition(".")[0])
    allow.add("__future__")

    files = _module_files(mod)

    if not files:
        raise AssertionError(f"Purity gate for {mod.__name__!r} discovered no source files")

    violations: list[str] = []

    for path in files:
        for lineno, root in _import_roots(ast.parse(path.read_text(), filename=str(path))):
            if root in forbid:
                violations.append(f"{path}:{lineno}: import of forbidden module {root!r}")
            elif root not in allow:
                violations.append(f"{path}:{lineno}: import of unlisted module {root!r}")

    if violations:
        _fail(f"Purity gate failed for {mod.__name__!r}:", violations)


# ----------------------- #
# Scope-first gate


def _protocol_methods(proto: type) -> list[tuple[str, FunctionType]]:
    members = []

    for member in sorted(typing.get_protocol_members(proto)):
        func = inspect.getattr_static(proto, member, None)

        if inspect.isfunction(func):
            members.append((member, func))

    return members


def assert_scope_first(
    module: ModuleType | str,
    *,
    name: str,
    annotation: object,
    exclude: Iterable[str] = (),
) -> None:
    """Assert every Protocol method in ``module`` takes the ownership key first.

    For each ``typing.Protocol`` defined in the module, every method's first parameter
    after ``self`` must be called ``name``, annotated ``annotation``, **positional-only**
    and **undefaulted**. Positional-only is the whole mechanism: a keyword parameter can
    be omitted and filled by a default, and a naming convention is exactly what does not
    prevent a missing ``WHERE`` — a caller must be physically unable to leave the key out.

    ``exclude`` names deliberate exceptions as ``"ProtocolName.method"``; an exclusion
    matching nothing fails, so the list can only shrink. A module with no Protocols, or
    Protocols with no methods, fails rather than passing vacuously.
    """

    mod = _resolve_module(module)

    protocols = [
        obj
        for obj in vars(mod).values()
        if isinstance(obj, type) and obj.__module__ == mod.__name__ and typing.is_protocol(obj)
    ]

    if not protocols:
        raise AssertionError(f"Scope-first gate found no Protocols in {mod.__name__!r}")

    excluded = set(exclude)
    seen_exclusions: set[str] = set()
    violations: list[str] = []
    checked = 0

    for proto in protocols:
        for method, func in _protocol_methods(proto):
            qualified = f"{proto.__name__}.{method}"

            if qualified in excluded:
                seen_exclusions.add(qualified)
                continue

            checked += 1
            params = [
                p
                for p in inspect.signature(func, eval_str=True).parameters.values()
                if p.name != "self"
            ]

            if not params:
                violations.append(f"{qualified}: takes no parameter to carry {name!r}")
                continue

            first = params[0]

            if first.name != name:
                violations.append(f"{qualified}: first parameter is {first.name!r}, not {name!r}")
            if first.kind is not inspect.Parameter.POSITIONAL_ONLY:
                violations.append(f"{qualified}: {first.name!r} is not positional-only")
            if first.default is not inspect.Parameter.empty:
                violations.append(f"{qualified}: {first.name!r} carries a default")
            if first.annotation != annotation:
                violations.append(
                    f"{qualified}: {first.name!r} is annotated {first.annotation!r}, "
                    f"expected {annotation!r}",
                )

    stale = sorted(excluded - seen_exclusions)

    if stale:
        violations.extend(f"exclusion matches no method: {entry!r}" for entry in stale)

    if checked == 0 and not violations:
        raise AssertionError(
            f"Scope-first gate checked no methods in {mod.__name__!r} — every Protocol "
            "is empty or excluded",
        )

    if violations:
        _fail(f"Scope-first gate failed for {mod.__name__!r} (key {name!r}):", violations)


# ----------------------- #
# Operation-namespace gate


def assert_operation_namespaces(
    ids_by_edge: Mapping[str, Iterable[str]],
    *,
    separator: str = ".",
) -> None:
    """Assert each edge's operation ids live under its own namespace, disjointly.

    ``ids_by_edge`` maps an edge's namespace prefix to the operation ids it serves. Every
    id must start with its own edge's prefix plus ``separator``, and no id may appear on
    two edges. An empty mapping, or an edge whose enumeration came back empty, fails —
    an enumeration that silently broke is indistinguishable from a passing gate
    otherwise.
    """

    if not ids_by_edge:
        raise AssertionError("Operation-namespace gate received no edges")

    violations: list[str] = []
    owners: dict[str, str] = {}

    for edge, ids in ids_by_edge.items():
        id_set = set(ids)

        if not id_set:
            violations.append(f"edge {edge!r}: enumeration returned no operation ids")
            continue

        prefix = f"{edge}{separator}"

        for op_id in sorted(id_set):
            if not op_id.startswith(prefix):
                violations.append(f"edge {edge!r}: {op_id!r} is not under {prefix!r}")

            owner = owners.setdefault(op_id, edge)

            if owner != edge:
                violations.append(f"{op_id!r} appears on both {owner!r} and {edge!r}")

    if violations:
        _fail("Operation-namespace gate failed:", violations)
