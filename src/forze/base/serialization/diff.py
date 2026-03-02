"""Dict-diff and merge helpers used by higher-level composition logic."""

from copy import deepcopy
from itertools import chain
from typing import Any, Iterable, cast

from deepdiff import DeepDiff
from deepdiff.model import DiffLevel
from mergedeep import merge  # type: ignore[import-untyped]

from ..primitives.types import JsonDict

# ----------------------- #


def _set_nested(  # pragma: no cover
    dst: JsonDict,
    path: Iterable[Any],
    value: Any,
) -> None:
    cur = dst
    parts = list(path)

    for p in parts[:-1]:
        if isinstance(p, int):
            raise ValueError("List indexes are not supported in merge patch")

        cur = cur.setdefault(p, {})

    last = parts[-1]

    if isinstance(last, int):
        raise ValueError("List indexes are not supported in merge patch")

    cur[last] = value


# ....................... #


def _maybe_deepcopy(x: Any) -> Any:
    return deepcopy(x) if isinstance(x, (dict, list, set, tuple)) else x  # type: ignore[report-untyped-call]


# ....................... #


def _get_by_path(obj: Any, path: Iterable[Any]) -> Any:
    cur = obj

    for p in path:
        cur = cur[p]

    return cur


# ....................... #


def _iterate_deepdiff(dd: DeepDiff, group_name: str) -> tuple[DiffLevel, ...]:
    grp = dd.get(group_name) or ()  # type: ignore[report-untyped-call]
    grp = cast(tuple[DiffLevel, ...], grp)

    return grp


# ....................... #


def _parent_list_path(node: DiffLevel) -> list[str | int]:
    p = list(node.path(output_format="list"))

    while p and isinstance(p[-1], int):
        p.pop()

    return p


# ....................... #


def apply_dict_patch(before: JsonDict, patch: JsonDict) -> JsonDict:
    """Apply a merge-style JSON patch to ``before``.

    :param before: Original JSON-like dictionary.
    :param patch: Merge patch to apply.
    :returns: New dictionary with the patch applied.
    """

    before_copy = deepcopy(before)
    res = merge(before_copy, patch)  # type: ignore[report-untyped-call]

    return cast(JsonDict, res)


# ....................... #


def calculate_dict_difference(
    before: JsonDict,
    after: JsonDict,
    *,
    deletions_as_none: bool = True,
) -> JsonDict:
    """Calculate a JSON-merge-style patch representing ``after`` vs ``before``.

    The resulting patch can be applied with :func:`apply_dict_patch` to obtain
    ``after`` from ``before``. List changes are represented by replacing the
    entire parent list.

    :param before: Original JSON-like dictionary.
    :param after: Target JSON-like dictionary.
    :param deletions_as_none: When true, dictionary item deletions are encoded
        as ``None`` values in the patch instead of being omitted.
    :returns: Merge patch that transforms ``before`` into ``after``.
    """
    dd = DeepDiff(
        before,
        after,
        ignore_order=True,
        report_repetition=True,
        view="tree",
    )
    patch: JsonDict = {}

    for node in chain(
        _iterate_deepdiff(dd, "values_changed"),
        _iterate_deepdiff(dd, "type_changes"),
    ):
        p = list(node.path(output_format="list"))

        if any(isinstance(x, int) for x in p):
            lp = _parent_list_path(node)
            lst = _get_by_path(after, lp)

            _set_nested(patch, lp, _maybe_deepcopy(lst))

        _set_nested(patch, p, _maybe_deepcopy(node.t2))

    for node in _iterate_deepdiff(dd, "dictionary_item_added"):
        p = list(node.path(output_format="list"))
        _set_nested(patch, p, _maybe_deepcopy(_get_by_path(after, p)))

    if deletions_as_none:
        for node in _iterate_deepdiff(dd, "dictionary_item_removed"):
            p = list(node.path(output_format="list"))
            _set_nested(patch, p, None)

    for kind in (
        "iterable_item_added",
        "iterable_item_removed",
        "iterable_item_moved",
        "iterable_item_repetition_change",
    ):
        for node in _iterate_deepdiff(dd, kind):
            lp = _parent_list_path(node)
            lst = _get_by_path(after, lp)
            _set_nested(patch, lp, _maybe_deepcopy(lst))

    return patch


# ....................... #

DictPath = tuple[str, ...]
"""Path into a nested dict as a tuple of keys."""


def deep_dict_intersection(
    a: JsonDict,
    b: JsonDict,
    _prefix: DictPath = (),
) -> set[DictPath]:
    """Return the set of matching leaf key paths shared by both dictionaries.

    Recursively compares nested dicts; only paths where both values are
    non-dict (leaf) and equal are included.

    :param a: First dictionary.
    :param b: Second dictionary.
    :param _prefix: Internal recursion prefix; do not pass.
    :returns: Set of key paths where both dicts have the same leaf value.
    """
    res: set[DictPath] = set()

    for k in a.keys() & b.keys():
        va, vb = a[k], b[k]
        path = _prefix + (k,)

        if isinstance(va, dict) and isinstance(vb, dict):
            res |= deep_dict_intersection(va, vb, path)  # type: ignore[report-call-arg]

        elif not isinstance(va, dict) and not isinstance(vb, dict):
            res.add(path)

        else:
            continue

    return res


# ....................... #


def collect_touched_paths_from_patch(
    patch: JsonDict, *, atomic_containers: bool = True
) -> set[DictPath]:
    out: set[DictPath] = set()

    def walk(node: Any, prefix: DictPath) -> None:
        if isinstance(node, dict):
            for k, v in node.items():  # pyright: ignore[reportUnknownVariableType]
                if not isinstance(k, str):
                    k = str(k)  # pyright: ignore[reportUnknownArgumentType]

                p = prefix + (k,)

                if isinstance(v, dict):
                    if atomic_containers:
                        out.add(p)

                    else:
                        walk(v, p)

                out.add(p)

        out.add(prefix)

    walk(patch, ())
    out.discard(())

    return out


# ....................... #


def _is_prefix(a: DictPath, b: DictPath) -> bool:
    if len(a) > len(b):
        return False

    return b[: len(a)] == a


# ....................... #


def has_path_conflict(a: set[DictPath], b: set[DictPath]) -> bool:
    if not a or not b:
        return False

    a_sorted = sorted(a, key=len)
    b_sorted = sorted(b, key=len)

    for pa in a_sorted:
        for pb in b_sorted:
            if _is_prefix(pa, pb) or _is_prefix(pb, pa):
                return True

    return False
