"""Base64-JSON cursors and sort normalization (agnostic of SQL vs Mongo)."""

import base64
import hashlib
import hmac
from contextlib import contextmanager
from contextvars import ContextVar
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Sequence

import attrs

from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import exc

# ----------------------- #

_KEYSET_V1 = 1
_DIRECTIONS = ("asc", "desc")
_CODEC = B64UrlJsonCodec()
_DECIMAL_TAG = "$dec"
"""Wire tag round-tripping a ``Decimal`` sort key exactly (as its string form) so keyset
seek compares it numerically after decode, not as a bare (lexicographically-ordered) string."""

_SIGNATURE_SEP = "."
"""Separator between a signed token's ``base64url(payload)`` and its ``base64url(hmac)``. The
base64url alphabet excludes ``.``, so a single ``rpartition`` split is unambiguous."""


# ....................... #


def _b64url_nopad(data: bytes) -> str:
    """URL-safe base64 without ``=`` padding (matches the token codec's alphabet)."""

    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class CursorTokenSigner:
    """HMAC-SHA256 signer for opaque keyset cursor tokens — tamper-evidence, not secrecy.

    A signed token is ``<base64url payload>.<base64url hmac>``. Signing makes a client unable
    to forge keyset values (and, once the token carries a context binding, unable to replay it
    against a different query) — verification is constant-time. It does **not** hide the
    payload: the base64-JSON stays readable, so confidentiality (hiding boundary sort-key
    values) would need a separate AEAD layer.
    """

    secret: bytes = attrs.field(repr=False, validator=attrs.validators.min_len(32))
    """HMAC key; at least 32 bytes."""

    def sign(self, message: str) -> str:
        """Return the unpadded base64url HMAC-SHA256 of *message* (the encoded payload)."""

        mac = hmac.new(self.secret, message.encode("utf-8"), hashlib.sha256).digest()
        return _b64url_nopad(mac)

    def verify(self, message: str, signature: str) -> bool:
        """Constant-time check that *signature* is a valid HMAC of *message*."""

        return hmac.compare_digest(self.sign(message), signature)


# ....................... #


_cursor_signer_var: ContextVar[CursorTokenSigner | None] = ContextVar(
    "forze_cursor_token_signer", default=None
)
"""The active cursor-token signer for the current context (``None`` = unsigned, the default).

Context-scoped so two :class:`ExecutionRuntime` s in one process each mint/verify with their
own signer instead of sharing one — bind it with :func:`bind_cursor_signer` (the runtime does
this per scope) or :func:`configure_cursor_signer`. Every keyset mint/verify falls back to it
when no explicit ``signer`` is passed, so enabling it signs and requires signatures everywhere
at once — no per-backend wiring, no mint-signed/verify-unsigned split from a missed call site."""


def configure_cursor_signer(
    signer: CursorTokenSigner | None,
) -> CursorTokenSigner | None:
    """Set the cursor-token signer for the *current* context; return the previous one (restore).

    Opt-in: with a signer set, every keyset cursor token is HMAC-signed and verification
    rejects any unsigned or tampered token (a hard cutover — cursors minted before it 400
    once and the client restarts pagination). Call at startup, like ``configure_logging``; for
    a scoped binding that auto-restores (and isolates concurrent runtimes) use
    :func:`bind_cursor_signer`.
    """

    previous = _cursor_signer_var.get()
    _cursor_signer_var.set(signer)
    return previous


@contextmanager
def bind_cursor_signer(
    signer: CursorTokenSigner | None,
) -> Iterator[None]:
    """Bind *signer* as the cursor-token signer for the duration of the block, then restore.

    Context-scoped (a :class:`~contextvars.ContextVar`), so an :class:`ExecutionRuntime` binds
    its own signer per :meth:`~ExecutionRuntime.scope`: two runtimes in one process verify and
    mint with independent signers rather than clobbering a shared global.
    """

    token = _cursor_signer_var.set(signer)

    try:
        yield

    finally:
        _cursor_signer_var.reset(token)


def current_cursor_signer() -> CursorTokenSigner | None:
    """The cursor-token signer active in the current context, or ``None`` when unsigned."""

    return _cursor_signer_var.get()


def _effective_signer(signer: CursorTokenSigner | None) -> CursorTokenSigner | None:
    """An explicit *signer* wins; otherwise fall back to the context's active signer."""

    return signer if signer is not None else _cursor_signer_var.get()

# ....................... #


def _jsonify_value(v: Any) -> Any:
    if v is None:
        return None

    # Sort-key values are overwhelmingly primitives (ids, numbers, text) — handle
    # those before paying for ``type(v).__name__`` and the UUID/datetime/Decimal
    # name checks. No primitive type collides with those names, and none of those
    # types are primitive/list/dict instances, so the ordering is behavior-neutral.
    if isinstance(v, (str, int, float, bool)):
        return v

    if isinstance(v, (list, dict)):
        return v  # type: ignore[return-value]

    t = type(v).__name__

    if t in ("UUID", "uuid"):
        return str(v)

    if t in ("datetime", "date"):
        return v.isoformat()

    if t == "Decimal":
        # Tagged (not a bare string) so decode restores a ``Decimal`` and keyset seek
        # compares it numerically; a bare ``str(v)`` would order ``'9' > '10'``.
        return {_DECIMAL_TAG: str(v)}

    return str(v)


# ....................... #


def keyset_canonical_value(v: Any) -> Any:
    """Normalize a sort-key value to the wire form used in cursor tokens."""

    return _jsonify_value(v)


# ....................... #


def _compare_value(v: Any) -> Any:
    """Canonicalize a sort-key value for *comparison* — numbers stay numeric.

    Unlike the wire form (:func:`_jsonify_value`), an ``int`` / ``float`` / ``Decimal`` is
    coerced to ``Decimal`` so keys order numerically (``Decimal('9') < Decimal('10')``), not
    lexicographically as their string form would (``'9' > '10'``). UUID / datetime keep the
    string / isoformat canonicalization the cursor round-trip relies on.
    """

    if v is None or isinstance(v, bool):
        return v

    if isinstance(v, Decimal):
        return v

    if isinstance(v, (int, float)):
        return Decimal(str(v))

    if isinstance(v, (str, list, dict)):
        return v  # pyright: ignore[reportUnknownVariableType]

    t = type(v).__name__

    if t in ("UUID", "uuid"):
        return str(v)

    if t in ("datetime", "date"):
        return v.isoformat()

    return str(v)


# ....................... #


def compare_keyset_sort_values(left: Any, right: Any) -> int:
    """Compare two sort-key values (-1, 0, 1) using the numeric-aware canonicalization."""

    lc = _compare_value(left)
    rc = _compare_value(right)

    if lc == rc:
        return 0

    if lc is None:
        return -1

    if rc is None:
        return 1

    # Cursor values are client-controlled: a tampered token can put a value of
    # the wrong type next to a row value (e.g. ``int < str``) — surface that as
    # an invalid-cursor validation error instead of a raw TypeError (500).
    try:
        if lc < rc:
            return -1

    except TypeError as e:
        raise exc.validation("Invalid cursor token") from e

    return 1


# ....................... #


def ordered_compare(
    left: Any,
    right: Any,
    *,
    direction: str,
    nulls: str,
) -> int:
    """Sort-order comparison for one key: ``-1`` if *left* sorts before *right*, else.

    Null placement is *absolute* — ``nulls="first"`` puts nulls at the start, ``"last"``
    at the end, independent of *direction*; only the non-null comparison flips with
    direction. This is the canonical keyset order every backend conforms to.
    """

    lc = _compare_value(left)
    rc = _compare_value(right)
    l_null = lc is None
    r_null = rc is None

    if l_null:
        if r_null:
            return 0

        return -1 if nulls == "first" else 1

    if r_null:
        return 1 if nulls == "first" else -1

    if lc == rc:
        return 0

    # Cursor values are client-controlled: a tampered token can put a value of the wrong
    # type next to a row value — surface as an invalid-cursor error, not a raw TypeError.
    try:
        ordered = -1 if lc < rc else 1

    except TypeError as e:
        raise exc.validation("Invalid cursor token") from e

    return -ordered if direction == "desc" else ordered


# ....................... #


def _resolved_nulls(
    directions: Sequence[str],
    nulls: Sequence[str] | None,
) -> list[str]:
    """The explicit *nulls* placement, or the canonical default per direction.

    Supplied markers are lower-cased and validated so the result matches what
    :func:`decode_keyset_v1` produces and what :func:`ordered_compare` expects — this
    helper feeds both the encoded token and the seek-comparison path.
    """

    if nulls is None:
        return [_canonical_nulls(d) for d in directions]

    resolved = [str(n).lower() for n in nulls]

    for n in resolved:
        if n not in ("first", "last"):
            raise exc.internal(f"Invalid null placement {n!r}; expected 'first'/'last'")

    return resolved


def row_passes_keyset_seek(
    row: dict[str, Any],
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    cursor_values: Sequence[Any],
    after: bool,
    nulls: Sequence[str] | None = None,
) -> bool:
    """Return whether *row* is strictly after/before the cursor tuple (composite keyset)."""

    for key, direction, null_order, cursor_value in zip(
        sort_keys,
        directions,
        _resolved_nulls(directions, nulls),
        cursor_values,
        strict=True,
    ):
        cmp = ordered_compare(
            row_value_for_sort_key(row, key),
            cursor_value,
            direction=direction,
            nulls=null_order,
        )

        if cmp == 0:
            continue

        return cmp > 0 if after else cmp < 0

    return False


# ....................... #


def _parse_value(v: Any) -> Any:
    if v is None:
        return None

    if isinstance(v, (int, float, str, bool)):
        return v

    # The one accepted container is the Decimal tag (``{"$dec": "<digits>"}``), restored to
    # a ``Decimal`` so keyset seek compares it numerically. A malformed tag is a tampered
    # cursor, not a 500.
    if (
        isinstance(v, dict)
        and set(v) == {_DECIMAL_TAG}  # pyright: ignore[reportUnknownArgumentType]
        and isinstance(v[_DECIMAL_TAG], str)
    ):
        try:
            return Decimal(
                v[_DECIMAL_TAG]  # pyright: ignore[reportUnknownArgumentType]
            )

        except InvalidOperation as e:
            raise exc.validation("Invalid cursor token") from e

    # Token values are client-controlled: any other container (or non-scalar) is rejected
    # as a tampered cursor.
    raise exc.validation("Invalid cursor token")


# ....................... #


def _canonical_nulls(direction: str) -> str:
    """Default null placement for *direction* (asc → first, desc → last)."""

    return "first" if direction == "asc" else "last"


def encode_keyset_v1(
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    values: Sequence[Any],
    nulls: Sequence[str] | None = None,
    signer: CursorTokenSigner | None = None,
) -> str:
    null_order = _resolved_nulls(directions, nulls)

    if (
        len(sort_keys) != len(values)
        or len(sort_keys) != len(directions)
        or len(sort_keys) != len(null_order)
        or not sort_keys
    ):
        raise exc.internal(
            "Keyset token fields must be aligned in length and non-empty"
        )

    payload: dict[str, Any] = {
        "v": _KEYSET_V1,
        "k": list(sort_keys),
        "d": list(directions),
        "n": list(null_order),
        "x": [_jsonify_value(x) for x in values],
    }
    encoded = _CODEC.dumps(payload)
    signer = _effective_signer(signer)

    if signer is None:
        return encoded

    # Append the HMAC of the encoded payload; ``decode_keyset_v1`` verifies it before parsing.
    return f"{encoded}{_SIGNATURE_SEP}{signer.sign(encoded)}"


# ....................... #


def row_value_for_sort_key(row: dict[str, Any], key: str) -> Any:
    if "." not in key:
        return row.get(key)
    cur: Any = row
    for part in key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)  # type: ignore[assignment, misc]

    return cur  # type: ignore[return-value]


# ....................... #


def decode_keyset_v1(
    token: str,
    *,
    signer: CursorTokenSigner | None = None,
) -> tuple[list[str], list[str], list[str], list[Any]]:
    """Decode a keyset token to ``(keys, directions, nulls, values)``.

    A token written before per-key null placement existed carries no ``n`` field; its
    nulls default to the canonical placement for each direction, so old cursors stay
    valid as long as the active sort uses that default.

    When *signer* is set, the token must be signed and the HMAC must verify (constant-time);
    an unsigned or tampered token is rejected as invalid — a hard cutover, so enabling signing
    invalidates cursors minted without it (they 400 once and the client restarts pagination).
    """

    signer = _effective_signer(signer)

    if signer is not None:
        encoded, sep, signature = token.rpartition(_SIGNATURE_SEP)

        if not sep or not signer.verify(encoded, signature):
            raise exc.validation("Invalid cursor token")

        token = encoded

    try:
        data: Any = _CODEC.loads(token)

    except ValueError as e:
        raise exc.validation("Invalid cursor token") from e

    if not isinstance(data, dict) or int(data.get("v", 0)) != _KEYSET_V1:  # type: ignore[arg-type]
        raise exc.validation("Invalid cursor token")

    k = data.get("k")  # type: ignore[assignment, misc]
    d = data.get("d")  # type: ignore[assignment, misc]
    x = data.get("x")  # type: ignore[assignment, misc]

    if not isinstance(k, list) or not isinstance(d, list) or not isinstance(x, list):
        raise exc.validation("Invalid cursor token")

    if len(k) != len(d) or len(k) != len(x):  # type: ignore[arg-type]
        raise exc.validation("Invalid cursor token")

    keys = [str(a) for a in k]  # type: ignore[arg-type]
    dirs = [str(a).lower() for a in d]  # type: ignore[arg-type]

    for dr in dirs:
        if dr not in _DIRECTIONS:
            raise exc.validation("Invalid cursor token")

    n = data.get("n")  # type: ignore[assignment, misc]

    if n is None:
        nulls = [_canonical_nulls(dr) for dr in dirs]

    else:
        if not isinstance(n, list) or len(n) != len(k):  # type: ignore[arg-type]
            raise exc.validation("Invalid cursor token")

        nulls = [str(a).lower() for a in n]  # type: ignore[arg-type]

        for nn in nulls:
            if nn not in ("first", "last"):
                raise exc.validation("Invalid cursor token")

    vals = [_parse_value(v) for v in x]  # type: ignore[arg-type]

    return keys, dirs, nulls, vals


# ....................... #


def validate_cursor_token(
    token: str,
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    nulls: Sequence[str] | None = None,
    signer: CursorTokenSigner | None = None,
) -> list[Any]:
    """Decode a keyset *token* and verify it matches the active sort; return its values.

    Raises :func:`~forze.base.exceptions.exc.validation` when the token's keys,
    directions, or null placement do not align with the current search sort (a stale or
    mismatched cursor). When *nulls* is omitted the canonical placement is assumed.
    Shared by every keyset-cursor search path so the validation is identical. When *signer*
    is set the token's HMAC is verified first (see :func:`decode_keyset_v1`).
    """

    null_order = _resolved_nulls(directions, nulls)
    tk, td, tn, tv = decode_keyset_v1(token, signer=signer)

    if (
        list(tk) != list(sort_keys)
        or len(td) != len(directions)
        or len(tn) != len(null_order)
    ):
        raise exc.validation("Cursor does not match current search sort")

    for i, di in enumerate(directions):
        if (td[i] or "").lower() != di or (tn[i] or "").lower() != null_order[i]:
            raise exc.validation("Cursor does not match current search sort")

    return list(tv)


# ....................... #


def keyset_page_bounds(
    raw_rows: list[dict[str, Any]],
    limit: int,
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    use_after: bool,
    use_before: bool,
    nulls: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], bool, str | None, str | None]:
    """Trim an over-fetched keyset result to one page and compute next/prev cursors.

    *raw_rows* holds up to ``limit + 1`` rows — the extra row signals more pages. For a
    ``before`` page the rows are reversed back into ascending order first. Returns
    ``(rows, has_more, next_cursor, prev_cursor)``. Shared by the keyset-cursor search
    paths so the page-boundary and token-emission logic is single-sourced.
    """

    if use_before:
        raw_rows = list(reversed(raw_rows))

    has_more = len(raw_rows) > limit
    rows = raw_rows[:limit]

    def _row_token_vals(row: dict[str, Any]) -> list[Any]:
        return [row_value_for_sort_key(row, k) for k in sort_keys]

    nxt = (
        encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=_row_token_vals(rows[-1]),
        )
        if has_more and rows
        else None
    )

    prv = (
        encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=_row_token_vals(rows[0]),
        )
        if rows and (use_after or (use_before and has_more))
        else None
    )

    return rows, has_more, nxt, prv
