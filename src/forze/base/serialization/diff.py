"""Dict-diff and merge helpers used by higher-level composition logic."""

import logging
from copy import deepcopy
from typing import Any, Iterable, cast

from ..errors import CoreError
from ..logging import log_section
from ..primitives.types import JsonDict

# ----------------------- #

logger = logging.getLogger(__name__)

# ....................... #


def _set_nested(
    dst: JsonDict,
    path: Iterable[Any],
    value: Any,
) -> None:
    """Set a deeply nested key in *dst* following *path*."""

    cur = dst
    parts = list(path)

    for p in parts[:-1]:
        if isinstance(p, int):
            raise CoreError("List indexes are not supported in merge patch")

        cur = cur.setdefault(p, {})

    last = parts[-1]

    if isinstance(last, int):
        raise CoreError("List indexes are not supported in merge patch")

    cur[last] = value


# ....................... #


def _shallow_merge(base: JsonDict, patch: JsonDict) -> JsonDict:
    """Recursively merge *patch* into *base*, returning a new dict."""

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

    logger.debug("Applying dict patch")

    with log_section():
        logger.debug("Before: %s", before)
        logger.debug("Patch: %s", patch)

        res = _shallow_merge(before, patch)

        logger.debug("Result: %s", res)

    return res


# ....................... #


def _diff_recursive(
    before: Any,
    after: Any,
    patch: JsonDict,
    path: tuple[str, ...],
    deletions_as_none: bool,
) -> None:
    """Walk *before* and *after* recursively, collecting changes into *patch*."""

    logger.debug("Diff node at %s", path or ("<root>",))

    with log_section():
        if isinstance(before, dict) and isinstance(after, dict):
            after = cast(dict[str, Any], after)
            before = cast(dict[str, Any], before)

            for k in after:
                child_path = path + (k,)

                if k not in before:
                    logger.debug("Added key at %s", child_path)
                    _set_nested(patch, child_path, deepcopy(after[k]))

                else:
                    _diff_recursive(
                        before[k],
                        after[k],
                        patch,
                        child_path,
                        deletions_as_none,
                    )

            if deletions_as_none:
                for k in before:
                    if k not in after:
                        logger.debug("Removed key at %s", path + (k,))
                        _set_nested(patch, path + (k,), None)

            return

        if isinstance(before, list) and isinstance(after, list):
            before = cast(list[Any], before)  # type: ignore[redundant-cast]
            after = cast(list[Any], after)  # type: ignore[redundant-cast]

            if before != after:
                logger.debug("Replaced list at %s", path)
                _set_nested(patch, path, deepcopy(after))

            return

        if before != after:
            logger.debug("Changed value at %s", path)
            _set_nested(
                patch,
                path,
                deepcopy(after) if isinstance(after, (dict, list, set, tuple)) else after,  # type: ignore[arg-type]
            )


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

    logger.debug(
        "Calculating dict difference (deletions_as_none=%s)",
        deletions_as_none,
    )

    with log_section():
        logger.debug("Before: %s", before)
        logger.debug("After: %s", after)

        patch: JsonDict = {}
        _diff_recursive(before, after, patch, (), deletions_as_none)

        logger.debug("Calculated patch: %s", patch)

    return patch


# ....................... #

DictPath = tuple[str, ...]
"""Path into a nested dict as a tuple of keys."""


def _is_prefix(a: DictPath, b: DictPath) -> bool:
    """Return ``True`` if *a* is a prefix of *b*."""

    if len(a) > len(b):
        return False

    return b[: len(a)] == a


def is_prefix(a: DictPath, b: DictPath) -> bool:
    """Return ``True`` if either path is a prefix of the other."""

    return _is_prefix(a, b) or _is_prefix(b, a)


# ....................... #


def split_touches_from_merge_patch(
    patch: JsonDict,
) -> tuple[dict[DictPath, Any], set[DictPath]]:
    """Split a merge patch into scalar and container path sets.

    :param patch: JSON-merge-style patch dictionary.
    :returns: A tuple of scalar-path→value mapping and a set of container paths.
    """

    logger.debug("Splitting touches from merge patch")

    scalar_map: dict[DictPath, Any] = {}
    container_paths: set[DictPath] = set()

    with log_section():
        logger.debug("Patch: %s", patch)

        def walk(node: Any, prefix: DictPath) -> None:
            if isinstance(node, dict):
                for k, v in node.items():  # pyright: ignore[reportUnknownVariableType]
                    k = str(k)  # pyright: ignore[reportUnknownArgumentType]
                    p = prefix + (k,)

                    if isinstance(v, (dict, list)):
                        logger.debug("Container touch at %s", p)
                        container_paths.add(p)

                    else:
                        logger.debug("Scalar touch at %s", p)
                        scalar_map[p] = v

                return

            if prefix and isinstance(node, (dict, list)):
                logger.debug("Container touch at %s", prefix)
                container_paths.add(prefix)

            else:
                logger.debug("Scalar touch at %s", prefix)
                scalar_map[prefix] = node

        walk(patch, ())

        logger.debug("Scalar map: %s", scalar_map)
        logger.debug("Container paths: %s", container_paths)

    return scalar_map, container_paths


# ....................... #


def has_hybrid_patch_conflict(
    a_scalars: dict[DictPath, Any],
    a_containers: set[DictPath],
    b_scalars: dict[DictPath, Any],
    b_containers: set[DictPath],
) -> bool:
    """Return ``True`` if two patches touch overlapping paths.

    Used to detect merge-patch conflicts where concurrent patches modify
    the same or ancestor/descendant key paths.
    """

    logger.debug("Checking for hybrid patch conflict")

    with log_section():
        logger.debug("A scalars: %s", a_scalars)
        logger.debug("A containers: %s", a_containers)
        logger.debug("B scalars: %s", b_scalars)
        logger.debug("B containers: %s", b_containers)

        all_a = set(a_containers) | set(a_scalars.keys())
        all_b = set(b_containers) | set(b_scalars.keys())

        for pa in all_a:
            for pb in all_b:
                if not is_prefix(pa, pb):
                    continue

                is_both_scalar = pa in a_scalars and pb in b_scalars

                if is_both_scalar and pa == pb and a_scalars[pa] == b_scalars[pb]:
                    logger.debug("Compatible scalar overlap at %s", pa)
                    continue

                logger.debug("Conflict detected between %s and %s", pa, pb)
                return True

        logger.debug("No conflict detected")
        return False
