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


def deep_dict_intersection(
    a: JsonDict,
    b: JsonDict,
    _prefix: DictPath = (),
) -> set[DictPath]:
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
