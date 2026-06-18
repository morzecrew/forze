"""Guard: no module crashes on import under runtime-evaluated annotations.

Python < 3.14 evaluates class/module annotations at definition time (no PEP 649). A field
annotated with a name imported only under ``if TYPE_CHECKING`` then raises ``NameError`` *at
import* unless the module either quotes the annotation or carries ``from __future__ import
annotations``. On 3.14 this is masked (annotations are lazy), so a normal import test on 3.14
would not catch it — this AST scan is version-independent.
"""

from __future__ import annotations

import ast
from pathlib import Path

# ----------------------- #

_SRC = Path(__file__).resolve().parents[1].parent / "src"


def _type_checking_only_names(tree: ast.Module) -> set[str]:
    """Names imported only inside an ``if TYPE_CHECKING:`` block (unbound at runtime)."""

    names: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue

        test = node.test
        guards_type_checking = (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
            isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
        )

        if not guards_type_checking:
            continue

        for inner in ast.walk(ast.Module(body=node.body, type_ignores=[])):
            if isinstance(inner, ast.ImportFrom):
                names.update(alias.asname or alias.name for alias in inner.names)

    return names


def _has_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


def test_no_unquoted_type_checking_annotations_without_future_import() -> None:
    offenders: list[str] = []

    for path in _SRC.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - not our concern here
            continue

        if _has_future_annotations(tree):
            continue  # postponed evaluation makes every annotation a safe string

        type_checking = _type_checking_only_names(tree)

        if not type_checking:
            continue

        # An *unquoted* annotation that references a TYPE_CHECKING-only name is evaluated at
        # definition time on Python < 3.14 → NameError on import. (Quoted annotations are
        # strings and stay safe.)
        for node in ast.walk(tree):
            if isinstance(node, ast.AnnAssign) and node.annotation is not None:
                if any(
                    isinstance(ref, ast.Name) and ref.id in type_checking
                    for ref in ast.walk(node.annotation)
                ):
                    offenders.append(f"{path}: {node.target.lineno}")
                    break

    assert not offenders, (
        "Modules reference a TYPE_CHECKING-only name in a runtime-evaluated annotation "
        "without `from __future__ import annotations` — these crash on import under "
        f"Python < 3.14. Add the future import or quote the annotation:\n  "
        + "\n  ".join(offenders)
    )
