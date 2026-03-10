"""Dict-diff and merge helpers used by higher-level composition logic."""

from copy import deepcopy
from typing import Any, Iterable

from ..primitives.types import JsonDict

# ----------------------- #


def _set_nested(
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


def _shallow_merge(base: JsonDict, patch: JsonDict) -> JsonDict:
    out = dict(base)
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _shallow_merge(out[k], v)  # type: ignore[arg-type]
        else:
            out[k] = v
    return out


# ....................... #


def apply_dict_patch(before: JsonDict, patch: JsonDict) -> JsonDict:
    """Apply a merge-style JSON patch to ``before``.

    :param before: Original JSON-like dictionary.
    :param patch: Merge patch to apply.
    :returns: New dictionary with the patch applied.
    """

    return _shallow_merge(before, patch)


# ....................... #


def _diff_recursive(
    before: Any,
    after: Any,
    patch: JsonDict,
    path: tuple[str, ...],
    deletions_as_none: bool,
) -> None:
    if isinstance(before, dict) and isinstance(after, dict):
        for k in after:
            child_path = path + (k,)
            if k not in before:
                _set_nested(patch, child_path, deepcopy(after[k]))
            else:
                _diff_recursive(before[k], after[k], patch, child_path, deletions_as_none)

        if deletions_as_none:
            for k in before:
                if k not in after:
                    _set_nested(patch, path + (k,), None)
        return

    if isinstance(before, list) and isinstance(after, list):
        if before != after:
            _set_nested(patch, path, deepcopy(after))
        return

    if before != after:
        _set_nested(patch, path, deepcopy(after) if isinstance(after, (dict, list, set, tuple)) else after)


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
    patch: JsonDict = {}
    _diff_recursive(before, after, patch, (), deletions_as_none)
    return patch


# ....................... #

DictPath = tuple[str, ...]
"""Path into a nested dict as a tuple of keys."""


def _is_prefix(a: DictPath, b: DictPath) -> bool:
    if len(a) > len(b):
        return False

    return b[: len(a)] == a


def is_prefix(a: DictPath, b: DictPath) -> bool:
    return _is_prefix(a, b) or _is_prefix(b, a)


# ....................... #


def split_touches_from_merge_patch(
    patch: JsonDict,
) -> tuple[dict[DictPath, Any], set[DictPath]]:
    scalar_map: dict[DictPath, Any] = {}
    container_paths: set[DictPath] = set()

    def walk(node: Any, prefix: DictPath) -> None:
        if isinstance(node, dict):
            for k, v in node.items():  # pyright: ignore[reportUnknownVariableType]
                k = str(k)  # pyright: ignore[reportUnknownArgumentType]
                p = prefix + (k,)

                if isinstance(v, (dict, list)):
                    container_paths.add(p)

                else:
                    scalar_map[p] = v

            return

        if prefix and isinstance(node, (dict, list)):
            container_paths.add(prefix)

        else:
            scalar_map[prefix] = node

    walk(patch, ())

    return scalar_map, container_paths


# ....................... #


def has_hybrid_patch_conflict(
    a_scalars: dict[DictPath, Any],
    a_containers: set[DictPath],
    b_scalars: dict[DictPath, Any],
    b_containers: set[DictPath],
) -> bool:
    all_a = set(a_containers) | set(a_scalars.keys())
    all_b = set(b_containers) | set(b_scalars.keys())

    for pa in all_a:
        for pb in all_b:
            if not is_prefix(pa, pb):
                continue

            is_both_scalar = pa in a_scalars and pb in b_scalars
            if is_both_scalar and pa == pb and a_scalars[pa] == b_scalars[pb]:
                continue

            return True

    return False
