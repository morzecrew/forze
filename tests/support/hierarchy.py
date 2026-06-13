"""Cross-backend parity for the hierarchy operators (``$descendant_of`` / ``$ancestor_of``).

A ``TreePath`` field holds a dot-separated materialized path. The operators ask, label
boundary aware and **inclusive**, whether a row's path is at-or-below (``$descendant_of``)
or at-or-above (``$ancestor_of``) a given node. The in-memory mock is the oracle (label
sequence prefix); Postgres renders native ``ltree`` containment (``@>`` / ``<@``) on an
``ltree`` column and a ``starts_with`` label-prefix fallback on a plain ``text`` column.
Backends that don't advertise ``supports_hierarchy`` (Mongo, Firestore, Meilisearch)
reject the operators up front and so are not exercised here.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from forze.application.contracts.querying import TreePath
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _TreeFields(BaseModel):
    label: str
    path: TreePath


class TreeCreate(CreateDocumentCmd, _TreeFields):
    pass


class TreeDoc(Document, _TreeFields):
    pass


class TreeRead(ReadDocument, _TreeFields):
    pass


# A small taxonomy:
#   top
#   ├── top.science
#   │   ├── top.science.math
#   │   │   └── top.science.math.algebra
#   │   └── top.science.physics
#   ├── top.scientist            (label-boundary trap for "top.science")
#   └── top.arts
SEED: tuple[TreeCreate, ...] = (
    TreeCreate(label="root", path=TreePath("top")),
    TreeCreate(label="science", path=TreePath("top.science")),
    TreeCreate(label="math", path=TreePath("top.science.math")),
    TreeCreate(label="algebra", path=TreePath("top.science.math.algebra")),
    TreeCreate(label="physics", path=TreePath("top.science.physics")),
    TreeCreate(label="scientist", path=TreePath("top.scientist")),
    TreeCreate(label="arts", path=TreePath("top.arts")),
)

# (filter, expected matching labels) — the oracle is the source of truth, but pinning the
# labels documents the intended label-boundary + inclusivity semantics.
CASES: tuple[tuple[dict[str, Any], set[str]], ...] = (
    # descendant_of: at or below the node (inclusive); "scientist" must NOT match.
    (
        {"$values": {"path": {"$descendant_of": "top.science"}}},
        {"science", "math", "algebra", "physics"},
    ),
    # a leaf is its own descendant
    (
        {"$values": {"path": {"$descendant_of": "top.science.math.algebra"}}},
        {"algebra"},
    ),
    # ancestor_of: at or above the node (inclusive)
    (
        {"$values": {"path": {"$ancestor_of": "top.science.math"}}},
        {"root", "science", "math"},
    ),
    # the root is an ancestor of everything (including itself)
    (
        {"$values": {"path": {"$ancestor_of": "top.arts"}}},
        {"root", "arts"},
    ),
    # list operand = "any": union of two subtrees
    (
        {"$values": {"path": {"$descendant_of": ["top.arts", "top.science.physics"]}}},
        {"arts", "physics"},
    ),
    # composition: descendants of top.science excluding the math subtree, via $not
    (
        {
            "$and": [
                {"$values": {"path": {"$descendant_of": "top.science"}}},
                {"$not": {"$values": {"path": {"$descendant_of": "top.science.math"}}}},
            ],
        },
        {"science", "physics"},
    ),
    # equality still works on a TreePath (it is a string)
    (
        {"$values": {"path": {"$eq": "top"}}},
        {"root"},
    ),
)


async def seed_tree_corpus(cmd: Any) -> None:
    """Create the taxonomy rows through *cmd* (a document command gateway or mock)."""

    for create in SEED:
        await cmd.create(create)


async def _labels(query: Any, filt: dict[str, Any]) -> set[str]:
    page = await query.find_page(filt, pagination={"limit": 100, "offset": 0})
    return {row.label for row in page.hits}


async def assert_hierarchy_parity(real_query: Any, oracle: Any) -> None:
    """Assert *real_query* reproduces the mock *oracle* for every hierarchy case.

    Both must already be seeded with :data:`SEED`. Each case is checked against the
    oracle (the canonical result) *and* against the hand-pinned expected label set.
    """

    for filt, expected in CASES:
        real_labels = await _labels(real_query, filt)
        oracle_labels = await _labels(oracle, filt)

        assert real_labels == oracle_labels, (
            f"{filt}: real {sorted(real_labels)} != oracle {sorted(oracle_labels)}"
        )
        assert real_labels == expected, (
            f"{filt}: real {sorted(real_labels)} != expected {sorted(expected)}"
        )
