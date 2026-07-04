"""Base64-JSON cursors and sort normalization (agnostic of SQL vs Mongo)."""

import base64
import hashlib
import hmac
import json
from contextlib import contextmanager
from contextvars import ContextVar
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Sequence, cast

import attrs

from forze.base.codecs import B64UrlJsonCodec
from forze.base.crypto import Aead, AesGcmAead
from forze.base.exceptions import CoreException, exc

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


def _b64url_decode(s: str) -> bytes:
    """Inverse of :func:`_b64url_nopad` — re-pad and decode URL-safe base64."""

    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


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


_ENC_PREFIX = "~"
"""Leading marker of an AEAD-encrypted cursor token. Outside the base64url alphabet and
distinct from the signature separator, so decode tells an encrypted token from a signed or
plaintext one at a glance (a plaintext base64url payload never starts with it)."""

_CURSOR_CIPHER_AAD = b"forze.cursor.token.v1"
"""Fixed associated data domain-separating cursor-token AEAD from any other use of the key."""

_CURSOR_KEY_INFO = b"forze.cursor.token.aead-key"
"""Domain-separation label for deriving the AES key from the configured secret."""

_NONCE_LEN = 12
"""GCM / ChaCha20-Poly1305 nonce length (see :mod:`forze.base.crypto`)."""


@attrs.define(frozen=True, slots=True, kw_only=True)
class CursorTokenCipher:
    """AEAD sealer for keyset cursor tokens — confidentiality **and** integrity.

    An encrypted token is ``~<base64url(nonce || ciphertext+tag)>``: the whole keyset payload
    (sort keys, boundary values, and any context binding) is hidden, so a client can't read the
    boundary sort-key values — which may not appear in the row projection — or introspect the
    cursor internals. The AEAD tag authenticates too, so an encrypted token needs no separate
    HMAC: a cipher **supersedes** a :class:`CursorTokenSigner` when both are configured.

    The AES-256 key is derived from *secret* (like the signer's), so this is a static-secret
    cipher, not a KMS-backed one; rotating the secret invalidates in-flight cursors (they 400
    once and the client restarts). Defaults to AES-256-GCM via :mod:`forze.base.crypto`, whose
    nonce comes from the ambient entropy source (deterministic under simulation).
    """

    secret: bytes = attrs.field(repr=False, validator=attrs.validators.min_len(32))
    """Root secret; at least 32 bytes. The AES-256 key is derived from it by domain-separated
    SHA-256, so the secret need not itself be exactly 32 bytes."""

    aead: Aead = attrs.field(factory=AesGcmAead, repr=False)
    """AEAD primitive (default AES-256-GCM); swap for ChaCha20-Poly1305 on non-AES-NI hosts."""

    _key: bytes = attrs.field(
        init=False,
        repr=False,
        default=attrs.Factory(
            lambda self: hashlib.sha256(self.secret + _CURSOR_KEY_INFO).digest(),
            takes_self=True,
        ),
    )
    """The 32-byte AES-256 key, derived once from :attr:`secret` (domain-separated SHA-256)."""

    def seal(self, plaintext: str) -> str:
        """Encrypt *plaintext* (the encoded keyset payload) into a token body (no prefix)."""

        nonce, ciphertext = self.aead.seal(
            key=self._key,
            plaintext=plaintext.encode("utf-8"),
            aad=_CURSOR_CIPHER_AAD,
        )
        return _b64url_nopad(nonce + ciphertext)

    def open(self, body: str) -> str:
        """Decrypt a token body from :meth:`seal`; tampered / foreign -> ``validation`` error."""

        try:
            raw = _b64url_decode(body)

        except ValueError as e:
            raise exc.validation("Invalid cursor token") from e

        if len(raw) <= _NONCE_LEN:
            raise exc.validation("Invalid cursor token")

        try:
            plaintext = self.aead.open(
                key=self._key,
                nonce=raw[:_NONCE_LEN],
                ciphertext=raw[_NONCE_LEN:],
                aad=_CURSOR_CIPHER_AAD,
            )

        except CoreException as e:
            # AEAD auth failure / key-size mismatch — a tampered or foreign cursor. Normalize
            # to the uniform cursor message rather than leaking the crypto-layer detail.
            raise exc.validation("Invalid cursor token") from e

        return plaintext.decode("utf-8")


# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class CursorBinding:
    """The query context a keyset cursor is minted against — spec, tenant, and filter.

    A signed token embeds this binding's :meth:`digest`; verification recomputes the digest
    from the *current* request's (spec, tenant, filter) and rejects a mismatch. That stops a
    validly-signed cursor from being replayed against a different query — a different search
    spec, another tenant, or a changed filter. Sort is already bound structurally by
    :func:`validate_cursor_token` (keys/directions/nulls), so it is not repeated here.

    Only meaningful once signing is on: unsigned, a client could recompute a matching digest,
    so binding is defense-in-depth over the HMAC, not a standalone control. Any field may be
    ``None`` when a call site cannot supply it (e.g. the generic document read path has no
    spec) — mint and verify for one path use the same fields, so a partial binding still holds.
    """

    spec_name: str | None = None
    """The search/query spec name, or ``None`` for the generic (spec-less) document path."""

    tenant_id: str | None = None
    """The bound tenant (stringified), or ``None`` when the gateway is not tenant-aware."""

    filter_fingerprint: str | None = None
    """A deterministic digest of the parsed filter (see :func:`fingerprint_filter`)."""

    def digest(self) -> str:
        """A stable, PYTHONHASHSEED-independent digest embedded in the token as ``b``."""

        blob = json.dumps(
            {"s": self.spec_name, "t": self.tenant_id, "f": self.filter_fingerprint},
            sort_keys=True,
            separators=(",", ":"),
        )
        return _b64url_nopad(hashlib.sha256(blob.encode("utf-8")).digest())


# ....................... #


def _canonical_filter_value(v: Any) -> Any:
    """Canonicalize a filter operand into a deterministic, JSON-serializable form.

    Mirrors :func:`_jsonify_value` for scalars (``Decimal`` tagged, ``datetime``/``UUID``
    stringified) but also normalizes membership containers: a ``list``/``tuple`` keeps its
    order, a ``set``/``frozenset`` is **sorted** by canonical string form so its
    (hash-ordered, PYTHONHASHSEED-dependent) iteration order can't perturb the fingerprint.
    """

    if isinstance(v, (list, tuple)):
        return [_canonical_filter_value(x) for x in cast(Sequence[Any], v)]

    if isinstance(v, (set, frozenset)):
        items = [_canonical_filter_value(x) for x in cast(set[Any], v)]
        return sorted(items, key=lambda c: json.dumps(c, sort_keys=True, default=str))

    return _jsonify_value(v)


# ....................... #


def _canonical_filter_node(node: Any) -> Any:
    """Serialize a parsed filter AST node to nested lists of canonical primitives.

    Duck-typed on the node class name (like the value canonicalization above) so this
    codec stays free of a dependency on the filter-AST module. Combinator child order is
    preserved as written — the same filter dict fingerprints identically across pages and
    processes, which is all the binding needs.
    """

    if node is None:
        return None

    t = type(node).__name__

    if t == "QueryAnd":
        return ["and", [_canonical_filter_node(i) for i in node.items]]

    if t == "QueryOr":
        return ["or", [_canonical_filter_node(i) for i in node.items]]

    if t == "QueryNot":
        return ["not", _canonical_filter_node(node.item)]

    if t == "QueryField":
        return ["field", node.name, str(node.op), _canonical_filter_value(node.value)]

    if t == "QueryCompare":
        return ["cmp", node.left, str(node.op), node.right]

    if t == "QueryElem":
        return [
            "elem",
            node.path,
            str(node.quantifier),
            _canonical_filter_node(node.inner),
        ]

    # Unknown node: fold its (deterministic, hash-free) repr so an unexpected shape still
    # changes the fingerprint when the filter changes — never silently collapse to a constant.
    return ["?", str(node)]


# ....................... #


def fingerprint_filter(expr: Any) -> str:
    """A deterministic, PYTHONHASHSEED-independent digest of a parsed filter (``None`` ok).

    Canonicalizes the AST (:func:`_canonical_filter_node`) then hashes its compact,
    key-sorted JSON with SHA-256. Computed identically at mint and verify from the same
    parsed filter, so it is the ``filter`` leg of a :class:`CursorBinding`.
    """

    blob = json.dumps(
        _canonical_filter_node(expr),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )

    return _b64url_nopad(hashlib.sha256(blob.encode("utf-8")).digest())


# ....................... #


def build_cursor_binding(
    *,
    spec_name: str | None = None,
    tenant_id: Any | None = None,
    filter_expr: Any = None,
) -> CursorBinding:
    """Assemble a :class:`CursorBinding` from a spec name, tenant, and parsed filter.

    Both the mint and verify sites of one keyset path call this with the same inputs so the
    embedded and recomputed digests agree; *tenant_id* is stringified so a ``UUID`` and its
    string form bind identically.
    """

    return CursorBinding(
        spec_name=spec_name,
        tenant_id=None if tenant_id is None else str(tenant_id),
        filter_fingerprint=fingerprint_filter(filter_expr),
    )


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


# ....................... #


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


_cursor_cipher_var: ContextVar[CursorTokenCipher | None] = ContextVar(
    "forze_cursor_token_cipher", default=None
)
"""The active cursor-token cipher for the current context (``None`` = not encrypted).

Context-scoped like :data:`_cursor_signer_var`; an :class:`ExecutionRuntime` binds its own per
:meth:`~ExecutionRuntime.scope`. A cipher **supersedes** a signer — AEAD gives integrity too —
so with one bound every mint encrypts and every verify requires (and decrypts) an encrypted
token, everywhere at once, with no per-backend wiring."""


def configure_cursor_cipher(
    cipher: CursorTokenCipher | None,
) -> CursorTokenCipher | None:
    """Set the cursor-token cipher for the *current* context; return the previous one (restore).

    Opt-in: with a cipher set, every keyset cursor token is AEAD-encrypted (payload hidden,
    tag-authenticated) and verification rejects any unencrypted or tampered token — a hard
    cutover, like :func:`configure_cursor_signer`. A cipher takes precedence over any signer.
    """

    previous = _cursor_cipher_var.get()
    _cursor_cipher_var.set(cipher)
    return previous


@contextmanager
def bind_cursor_cipher(
    cipher: CursorTokenCipher | None,
) -> Iterator[None]:
    """Bind *cipher* as the cursor-token cipher for the duration of the block, then restore.

    Context-scoped (a :class:`~contextvars.ContextVar`), so two runtimes in one process encrypt
    and decrypt with independent ciphers rather than clobbering a shared global.
    """

    token = _cursor_cipher_var.set(cipher)

    try:
        yield

    finally:
        _cursor_cipher_var.reset(token)


def current_cursor_cipher() -> CursorTokenCipher | None:
    """The cursor-token cipher active in the current context, or ``None`` when not encrypting."""

    return _cursor_cipher_var.get()


def _effective_cipher(cipher: CursorTokenCipher | None) -> CursorTokenCipher | None:
    """An explicit *cipher* wins; otherwise fall back to the context's active cipher."""

    return cipher if cipher is not None else _cursor_cipher_var.get()


def cursor_protection_active() -> bool:
    """Whether cursor tokens are protected in this context — a signer **or** cipher is bound.

    Gates the (otherwise wasted) work of building a :class:`CursorBinding` at a mint/verify
    call site: the binding is embedded and checked only under signing or encryption, so with
    neither bound there is nothing to bind to and the filter need not be fingerprinted.
    """

    return _cursor_signer_var.get() is not None or _cursor_cipher_var.get() is not None


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
    cipher: CursorTokenCipher | None = None,
    binding: CursorBinding | None = None,
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
    signer = _effective_signer(signer)
    cipher = _effective_cipher(cipher)

    # The binding digest is embedded (and authenticated) only under integrity — a signer or a
    # cipher. Unprotected, a client could forge it, so it would be integrity theater; with
    # neither the token is byte-for-byte what it was before this feature.
    if binding is not None and (signer is not None or cipher is not None):
        payload["b"] = binding.digest()

    encoded = _CODEC.dumps(payload)

    # A cipher supersedes a signer: AEAD authenticates, so the sealed token needs no HMAC and
    # the whole payload (values + binding) is hidden.
    if cipher is not None:
        return f"{_ENC_PREFIX}{cipher.seal(encoded)}"

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
    cipher: CursorTokenCipher | None = None,
    binding: CursorBinding | None = None,
) -> tuple[list[str], list[str], list[str], list[Any]]:
    """Decode a keyset token to ``(keys, directions, nulls, values)``.

    A token written before per-key null placement existed carries no ``n`` field; its
    nulls default to the canonical placement for each direction, so old cursors stay
    valid as long as the active sort uses that default.

    When *cipher* is set the token must be AEAD-encrypted and is decrypted first (the tag
    authenticates); otherwise when *signer* is set the token must be signed and the HMAC must
    verify (constant-time). Either way an unprotected or tampered token is rejected — a hard
    cutover, so enabling protection invalidates cursors minted without it (they 400 once and the
    client restarts pagination).

    When *binding* is also given, the token's embedded binding digest must equal the current
    query's (spec, tenant, filter) — a valid cursor replayed against a different query is
    rejected. The digest is authenticated, so this check only runs under a signer or cipher.
    """

    signer = _effective_signer(signer)
    cipher = _effective_cipher(cipher)

    if cipher is not None:
        # Hard cutover: with a cipher configured, only an encrypted token is acceptable.
        if not token.startswith(_ENC_PREFIX):
            raise exc.validation("Invalid cursor token")

        token = cipher.open(token[len(_ENC_PREFIX) :])

    elif signer is not None:
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

    if binding is not None and (signer is not None or cipher is not None):
        embedded = data.get("b")  # type: ignore[assignment, misc]

        if not isinstance(embedded, str) or not hmac.compare_digest(
            embedded, binding.digest()
        ):
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
    cipher: CursorTokenCipher | None = None,
    binding: CursorBinding | None = None,
) -> list[Any]:
    """Decode a keyset *token* and verify it matches the active sort; return its values.

    Raises :func:`~forze.base.exceptions.exc.validation` when the token's keys,
    directions, or null placement do not align with the current search sort (a stale or
    mismatched cursor). When *nulls* is omitted the canonical placement is assumed.
    Shared by every keyset-cursor search path so the validation is identical. When *cipher*
    or *signer* is set the token is decrypted / HMAC-verified first, and when *binding* is set
    the token's context binding must match the current query (see :func:`decode_keyset_v1`).
    """

    null_order = _resolved_nulls(directions, nulls)
    tk, td, tn, tv = decode_keyset_v1(
        token, signer=signer, cipher=cipher, binding=binding
    )

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
    binding: CursorBinding | None = None,
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
            binding=binding,
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
            binding=binding,
        )
        if rows and (use_after or (use_before and has_more))
        else None
    )

    return rows, has_more, nxt, prv
