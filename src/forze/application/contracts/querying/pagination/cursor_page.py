"""Trim over-fetched rows and build next/prev keyset tokens for cursor pages."""

from typing import Any, Callable, Mapping, Sequence

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .cursor_token import CursorBinding, encode_keyset_v1, row_value_for_sort_key

# ----------------------- #

_DEFAULT_CURSOR_LIMIT = 10
MAX_CURSOR_LIMIT = 10_000
"""Ceiling for a client-supplied cursor page size. The limit is untrusted, so a huge value
(``limit=10**9``) is clamped rather than materializing an unbounded result set."""

# ....................... #


def _sort_key_in_projection(sort_key: str, return_fields: Sequence[str]) -> bool:
    """Whether the projection carries *sort_key*'s value for the cursor token.

    The keyset token is built from the **projected** row and read back by dotted path
    (:func:`row_value_for_sort_key`), so a sort key is served only by a return field that is
    the key itself or one of its ancestors: projecting ``address`` whole serves ``address.city``,
    and ``address.city`` serves itself. A *sibling* leaf (``address.zip`` for a sort on
    ``address.city``) shares the root but not the value — it would read back as ``None`` and
    seek the cursor from the wrong key — so the root alone is not enough.
    """

    return any(
        sort_key == field or sort_key.startswith(f"{field}.") for field in return_fields
    )


def assert_cursor_projection_includes_sort_keys(
    *,
    return_fields: Sequence[str] | None,
    sort_keys: Sequence[str],
) -> None:
    """Raise when projected fields can't supply a sort key used by keyset cursors.

    Each sort key must be covered by a return field equal to it or to one of its ancestors
    (project ``address`` whole, or ``address.city`` itself, to sort by ``address.city``);
    a sibling leaf sharing only the root does not count (see :func:`_sort_key_in_projection`).
    """
    if return_fields is None:
        return
    if all(_sort_key_in_projection(k, return_fields) for k in sort_keys):
        return
    raise exc.precondition(
        "When using return_fields with cursor list, the projection must include "
        "all sort and tie-breaker fields (including id): each sort key needs itself or "
        "an ancestor in return_fields (a sibling leaf sharing the root is not enough).",
    )


# ....................... #


def resolved_cursor_limit(cursor: Mapping[str, Any] | None) -> int:
    """Effective page size — default when omitted, coerced and clamped to ``[1, MAX_CURSOR_LIMIT]``.

    The limit is client-controlled, so a non-integer is a clean ``validation`` error (not a
    500 from a bare ``int('abc')``) and an over-large value is clamped to ``MAX_CURSOR_LIMIT``
    rather than materializing an unbounded result set (``LIMIT 1000000001``).
    """
    lim = dict(cursor or {}).get("limit")
    if lim is None:
        return _DEFAULT_CURSOR_LIMIT

    try:
        value = int(lim)

    # OverflowError: a non-finite float (``float('inf')``) — coercion must be a clean 400,
    # not a 500 from the raw ``int(inf)``.
    except (TypeError, ValueError, OverflowError) as e:
        raise exc.validation(
            "Cursor pagination 'limit' must be an integer"
        ) from e

    if value < 1:
        raise exc.validation("Cursor pagination 'limit' must be positive")

    return min(value, MAX_CURSOR_LIMIT)


# ....................... #


def assemble_keyset_cursor_page(
    fetched: Sequence[Any],
    *,
    cursor: Mapping[str, Any] | None,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    dump_row: Callable[[Any], JsonDict],
    nulls: Sequence[str] | None = None,
    binding: CursorBinding | None = None,
) -> tuple[list[Any], bool, str | None, str | None]:
    """Slice ``fetched`` to the requested window and derive opaque cursors.

    Gateways commonly return ``limit + 1`` rows so callers can infer
    ``has_more`` without a separate count query. A ``before`` page is fetched in flipped
    (descending-from-cursor) order and re-reversed by the gateway before it reaches here,
    so its over-fetch sentinel — the row *farthest* from the cursor — sits at the **front**
    of ``fetched``; the ``limit`` rows nearest the cursor are the tail. *nulls* (the
    per-key placement) is carried into the emitted tokens so a follow-up page validates;
    omit it for the canonical default. *binding* is embedded in the emitted tokens when
    signing is active so a follow-up page can prove it belongs to this query.
    """

    c = dict(cursor or {})
    lim = resolved_cursor_limit(c)

    use_after = c.get("after") is not None
    use_before = c.get("before") is not None

    has_more = len(fetched) > lim

    if use_before:
        # Keep the ``limit`` rows nearest the cursor (the tail — the gateway already
        # re-reversed the flipped fetch into ascending order, putting the sentinel first).
        # Slicing the head instead would keep the sentinel and drop the row adjacent to
        # the cursor (``before=5&limit=2`` over ``[1..5]`` -> ``[2,3]`` instead of ``[3,4]``).
        page_raw = list(fetched)[-lim:]
    else:
        page_raw = list(fetched)[:lim]

    if has_more and page_raw:
        last = dump_row(page_raw[-1])
        next_tok = encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=[row_value_for_sort_key(last, k) for k in sort_keys],
            binding=binding,
        )

    else:
        next_tok = None

    if page_raw and (use_after or (use_before and has_more)):
        first = dump_row(page_raw[0])
        prev_tok = encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=[row_value_for_sort_key(first, k) for k in sort_keys],
            binding=binding,
        )

    else:
        prev_tok = None

    return page_raw, has_more, next_tok, prev_tok
