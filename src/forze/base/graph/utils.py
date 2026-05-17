from typing import Hashable, Iterable

from ..errors import CoreError

# ----------------------- #


def predecessors[T: Hashable](
    nodes: Iterable[T],
    edges: Iterable[tuple[T, T]],
    *,
    u_before_v: bool = True,
) -> dict[T, set[T]]:
    """Build predecessor map for ``graphlib.TopologicalSorter``.

    If ``u_before_v=True`` (default), then each edge ``(u, v)`` is interpreted
    as ``v`` depends on ``u``, i.e. ``u -> v``.
    """

    preds: dict[T, set[T]] = {node: set() for node in nodes}
    universe = frozenset(preds)

    for a, b in edges:
        if a not in universe or b not in universe:
            raise CoreError(f"Edge ({a!r}, {b!r}) references unknown node")

        if a == b:
            raise CoreError(f"Edge ({a!r}, {b!r}) is a self-loop")

        u, v = (a, b) if u_before_v else (b, a)
        preds[v].add(u)

    return preds
