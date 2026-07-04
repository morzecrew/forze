"""Tests for :mod:`forze.application.contracts.querying.pagination.cursor_token`."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.querying.pagination.cursor_token import (
    _CODEC,
    _KEYSET_V1,
    compare_keyset_sort_values,
    keyset_canonical_value,
    decode_keyset_v1,
    encode_keyset_v1,
    keyset_page_bounds,
    ordered_compare,
    row_passes_keyset_seek,
    row_value_for_sort_key,
    validate_cursor_token,
)
from forze.application.contracts.querying.sort_resolution import (
    normalize_sorts_with_id,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.constants import ID_FIELD


def test_normalize_sorts_empty_defaults_id_asc() -> None:
    # Each key carries its canonical null placement (asc → first, desc → last).
    assert normalize_sorts_with_id(None) == [(ID_FIELD, "asc", "first")]
    assert normalize_sorts_with_id({}) == [(ID_FIELD, "asc", "first")]


def test_normalize_sorts_single_direction_appends_id_tiebreaker() -> None:
    assert normalize_sorts_with_id({"name": "asc"}) == [
        ("name", "asc", "first"),
        (ID_FIELD, "asc", "first"),
    ]
    assert normalize_sorts_with_id({"name": "desc", ID_FIELD: "desc"}) == [
        ("name", "desc", "last"),
        (ID_FIELD, "desc", "last"),
    ]


def test_normalize_sorts_mixed_directions_allowed() -> None:
    # Mixed asc/desc is supported — the composite seek compares each key in its own
    # direction. The id tie-breaker is appended (default asc → nulls first).
    assert normalize_sorts_with_id({"a": "asc", "b": "desc"}) == [
        ("a", "asc", "first"),
        ("b", "desc", "last"),
        (ID_FIELD, "asc", "first"),
    ]


def test_normalize_sorts_invalid_direction() -> None:
    with pytest.raises(CoreException, match="Invalid sort direction"):
        normalize_sorts_with_id({"name": "sideways"})  # type: ignore[dict-item]


def test_encode_decode_roundtrip_json_types() -> None:
    u = UUID("12345678-1234-5678-1234-567812345678")
    dt = datetime(2026, 4, 24, 12, 0, 0)
    d = date(2026, 4, 24)
    keys = ["name", "ts", "d", "dec", "flag", "n", ID_FIELD]
    dirs = ["asc"] * len(keys)
    values: list[object] = ["x", dt, d, Decimal("1.5"), True, 7, u]
    token = encode_keyset_v1(sort_keys=keys, directions=dirs, values=values)
    k2, d2, n2, v2 = decode_keyset_v1(token)
    assert k2 == keys
    assert d2 == ["asc"] * len(keys)
    assert n2 == ["first"] * len(keys)  # canonical for asc
    assert v2[0] == "x"
    assert v2[1] == dt.isoformat()
    assert v2[2] == d.isoformat()
    assert v2[3] == Decimal("1.5")  # Decimal round-trips exactly (not a bare string)
    assert isinstance(v2[3], Decimal)
    assert v2[4] is True
    assert v2[5] == 7
    assert v2[6] == str(u)


def test_decimal_keys_order_numerically_not_lexicographically() -> None:
    # The bug: canonicalizing Decimal to str then comparing gives "9" > "10". Keyset
    # order must be numeric, so 9 sorts *before* 10.
    assert ordered_compare(
        Decimal("9"), Decimal("10"), direction="asc", nulls="first"
    ) == -1
    assert ordered_compare(
        Decimal("10"), Decimal("9"), direction="asc", nulls="first"
    ) == 1
    assert compare_keyset_sort_values(Decimal("9"), Decimal("10")) == -1
    # Cross-type numeric comparison stays numeric (int/float/Decimal).
    assert compare_keyset_sort_values(9, Decimal("10")) == -1
    assert compare_keyset_sort_values(Decimal("9.5"), 10) == -1
    # A Decimal round-tripped through a token still compares numerically against a row value.
    token = encode_keyset_v1(sort_keys=["price"], directions=["asc"], values=[Decimal("10")])
    _, _, _, (cursor_price,) = decode_keyset_v1(token)
    assert compare_keyset_sort_values(Decimal("9"), cursor_price) == -1


def test_cursor_token_signer_round_trip_and_rejections() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
    )

    signer = CursorTokenSigner(secret=b"k" * 32)
    tok = encode_keyset_v1(
        sort_keys=["id"], directions=["asc"], values=[5], signer=signer
    )
    assert "." in tok  # <payload>.<signature>

    # Signed round-trip verifies and returns the values.
    assert decode_keyset_v1(tok, signer=signer)[3] == [5]

    # Tampered signature -> rejected.
    with pytest.raises(CoreException):
        decode_keyset_v1(tok[:-1] + ("x" if tok[-1] != "x" else "y"), signer=signer)

    # Wrong key -> rejected.
    with pytest.raises(CoreException):
        decode_keyset_v1(tok, signer=CursorTokenSigner(secret=b"y" * 32))

    # Hard cutover: an unsigned token under a signer -> rejected.
    unsigned = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[5])
    with pytest.raises(CoreException):
        decode_keyset_v1(unsigned, signer=signer)

    # A short secret is refused at construction.
    with pytest.raises(ValueError):
        CursorTokenSigner(secret=b"short")


def test_configured_cursor_signer_applies_without_explicit_param() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
        configure_cursor_signer,
    )

    previous = configure_cursor_signer(CursorTokenSigner(secret=b"k" * 32))

    try:
        # No explicit signer passed (as the gateways call it) -> the configured one applies.
        tok = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[7])
        assert "." in tok
        assert decode_keyset_v1(tok)[3] == [7]

    finally:
        configure_cursor_signer(previous)

    # Restored: unsigned again.
    assert "." not in encode_keyset_v1(
        sort_keys=["id"], directions=["asc"], values=[7]
    )


def test_bind_cursor_signer_scopes_and_restores() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
        bind_cursor_signer,
        current_cursor_signer,
    )

    assert current_cursor_signer() is None

    signer = CursorTokenSigner(secret=b"k" * 32)

    with bind_cursor_signer(signer):
        assert current_cursor_signer() is signer
        assert "." in encode_keyset_v1(
            sort_keys=["id"], directions=["asc"], values=[1]
        )

    # Auto-restored on block exit.
    assert current_cursor_signer() is None
    assert "." not in encode_keyset_v1(
        sort_keys=["id"], directions=["asc"], values=[1]
    )


@pytest.mark.asyncio
async def test_cursor_signer_is_isolated_across_concurrent_contexts() -> None:
    # The multi-runtime win: two concurrently-running contexts each bind their own signer and
    # never clobber each other (a module-global would let the last binding win for both).
    import asyncio

    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
        bind_cursor_signer,
        current_cursor_signer,
    )

    signer_a = CursorTokenSigner(secret=b"a" * 32)
    signer_b = CursorTokenSigner(secret=b"b" * 32)
    seen: dict[str, Any] = {}

    async def worker(name: str, signer: CursorTokenSigner) -> None:
        with bind_cursor_signer(signer):
            await asyncio.sleep(0)  # interleave with the other worker
            seen[name] = current_cursor_signer()
            seen[f"{name}_tok"] = encode_keyset_v1(
                sort_keys=["id"], directions=["asc"], values=[1]
            )

    await asyncio.gather(worker("a", signer_a), worker("b", signer_b))

    # Each worker kept its own signer despite interleaving.
    assert seen["a"] is signer_a
    assert seen["b"] is signer_b

    # And a token minted under A does not verify under B's signer.
    with pytest.raises(CoreException):
        decode_keyset_v1(seen["a_tok"], signer=signer_b)

    # Outside any binding, signing is off again.
    assert current_cursor_signer() is None


# ----------------------- #
# Context binding (spec + tenant + filter): P2


def _parse_filter(raw: object) -> object:
    from forze.application.contracts.querying.internal import (
        QueryFilterExpressionParser,
    )

    return QueryFilterExpressionParser.parse(raw)  # type: ignore[arg-type]


def test_fingerprint_filter_is_stable_and_filter_sensitive() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        fingerprint_filter,
    )

    open_f = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    open_again = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    closed_f = _parse_filter({"$values": {"status": {"$eq": "closed"}}})

    # Same filter -> same fingerprint (across independent parses); different filter differs.
    assert fingerprint_filter(open_f) == fingerprint_filter(open_again)
    assert fingerprint_filter(open_f) != fingerprint_filter(closed_f)

    # Empty filter has a single stable fingerprint.
    assert fingerprint_filter(None) == fingerprint_filter(None)
    assert fingerprint_filter(None) != fingerprint_filter(open_f)


def test_fingerprint_filter_membership_container_is_deterministic() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        _canonical_filter_value,
        fingerprint_filter,
    )

    a = _parse_filter({"$values": {"name": {"$in": ["a", "b", "c"]}}})
    b = _parse_filter({"$values": {"name": {"$in": ["a", "b", "c"]}}})
    assert fingerprint_filter(a) == fingerprint_filter(b)

    # A set operand is sorted into a stable order regardless of (hash-ordered) iteration.
    assert _canonical_filter_value({3, 1, 2}) == _canonical_filter_value({2, 3, 1})


def test_fingerprint_filter_covers_every_node_type() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        fingerprint_filter,
    )

    # One filter per AST node kind: $or (QueryOr), $not (QueryNot), $fields (QueryCompare),
    # and $any/$all element quantifiers (QueryElem) — each must fingerprint stably and differ
    # from the others.
    raw_by_kind = {
        "or": {"$or": [{"$values": {"a": {"$eq": 1}}}, {"$values": {"b": {"$eq": 2}}}]},
        "not": {"$not": {"$values": {"a": {"$eq": 1}}}},
        "cmp": {"$fields": {"a": {"$gt": "b"}}},
        "any": {"$values": {"tags": {"$any": "x"}}},
        "all": {"$values": {"nums": {"$all": {"$gte": 2}}}},
    }

    fps: set[str] = set()
    for raw in raw_by_kind.values():
        fp = fingerprint_filter(_parse_filter(raw))
        # Stable across an independent re-parse.
        assert fp == fingerprint_filter(_parse_filter(raw))
        fps.add(fp)

    # Every node kind yields a distinct fingerprint.
    assert len(fps) == len(raw_by_kind)


def test_fingerprint_filter_unknown_node_falls_back_deterministically() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        fingerprint_filter,
    )

    # A shape the canonicalizer does not recognize folds through its repr — still deterministic
    # (same value → same fingerprint) and still sensitive (a different value differs), never a
    # silent constant.
    class _Weird:
        def __init__(self, tag: str) -> None:
            self.tag = tag

        def __repr__(self) -> str:
            return f"_Weird({self.tag})"

    assert fingerprint_filter(_Weird("a")) == fingerprint_filter(_Weird("a"))
    assert fingerprint_filter(_Weird("a")) != fingerprint_filter(_Weird("b"))


def test_cursor_binding_digest_changes_per_dimension() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        build_cursor_binding,
    )

    f_open = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    f_closed = _parse_filter({"$values": {"status": {"$eq": "closed"}}})

    base = build_cursor_binding(spec_name="orders", tenant_id="t1", filter_expr=f_open)
    other_spec = build_cursor_binding(
        spec_name="invoices", tenant_id="t1", filter_expr=f_open
    )
    other_tenant = build_cursor_binding(
        spec_name="orders", tenant_id="t2", filter_expr=f_open
    )
    other_filter = build_cursor_binding(
        spec_name="orders", tenant_id="t1", filter_expr=f_closed
    )

    digests = {
        base.digest(),
        other_spec.digest(),
        other_tenant.digest(),
        other_filter.digest(),
    }
    # Every dimension (spec, tenant, filter) perturbs the digest independently.
    assert len(digests) == 4


def test_cursor_binding_tenant_uuid_and_str_are_equal() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        build_cursor_binding,
    )

    u = UUID("00000000-0000-0000-0000-0000000000ab")
    f = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    assert (
        build_cursor_binding(tenant_id=u, filter_expr=f).digest()
        == build_cursor_binding(tenant_id=str(u), filter_expr=f).digest()
    )


def test_signed_bound_cursor_rejects_cross_context_replay() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
        bind_cursor_signer,
        build_cursor_binding,
    )

    f_open = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    f_closed = _parse_filter({"$values": {"status": {"$eq": "closed"}}})
    minted = build_cursor_binding(spec_name="orders", tenant_id="t1", filter_expr=f_open)

    with bind_cursor_signer(CursorTokenSigner(secret=b"k" * 32)):
        tok = encode_keyset_v1(
            sort_keys=["id"], directions=["asc"], values=[5], binding=minted
        )
        # The exact same binding verifies and returns the values.
        assert validate_cursor_token(
            tok, sort_keys=["id"], directions=["asc"], binding=minted
        ) == [5]

        # Replay against a different spec / tenant / filter -> rejected, per dimension.
        for other in (
            build_cursor_binding(
                spec_name="invoices", tenant_id="t1", filter_expr=f_open
            ),
            build_cursor_binding(spec_name="orders", tenant_id="t2", filter_expr=f_open),
            build_cursor_binding(
                spec_name="orders", tenant_id="t1", filter_expr=f_closed
            ),
        ):
            with pytest.raises(CoreException):
                validate_cursor_token(
                    tok, sort_keys=["id"], directions=["asc"], binding=other
                )


def test_binding_is_a_hard_cutover_once_signing_is_on() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenSigner,
        bind_cursor_signer,
        build_cursor_binding,
    )

    f = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    b = build_cursor_binding(spec_name="orders", tenant_id="t1", filter_expr=f)

    with bind_cursor_signer(CursorTokenSigner(secret=b"k" * 32)):
        # A signed token minted WITHOUT a binding (no embedded ``b``)...
        unbound_tok = encode_keyset_v1(
            sort_keys=["id"], directions=["asc"], values=[5]
        )
        # ...still verifies where no binding is required (signing only).
        assert validate_cursor_token(
            unbound_tok, sort_keys=["id"], directions=["asc"]
        ) == [5]
        # ...but is rejected once a binding is required (the ``b`` is absent).
        with pytest.raises(CoreException):
            validate_cursor_token(
                unbound_tok, sort_keys=["id"], directions=["asc"], binding=b
            )


def test_binding_is_inert_and_invisible_when_unsigned() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        build_cursor_binding,
    )

    f = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    b = build_cursor_binding(spec_name="orders", tenant_id="t1", filter_expr=f)

    # No signer bound: passing a binding embeds nothing and the token is byte-identical to
    # the legacy unsigned token (the whole feature stays opt-in behind a configured signer).
    with_binding = encode_keyset_v1(
        sort_keys=["id"], directions=["asc"], values=[5], binding=b
    )
    legacy = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[5])
    assert with_binding == legacy

    # And an unsigned token verifies regardless of the binding passed (nothing to check).
    assert validate_cursor_token(
        with_binding, sort_keys=["id"], directions=["asc"], binding=b
    ) == [5]


def test_fingerprint_filter_is_stable_across_pythonhashseed() -> None:
    # PYTHONHASHSEED-independence is a hard requirement (the DST determinism guard forbids
    # hash()-ordering): the fingerprint of a set-bearing filter must be identical in two
    # interpreters started with different hash seeds.
    import os
    import subprocess
    import sys

    prog = (
        "from forze.application.contracts.querying.internal import "
        "QueryFilterExpressionParser as P;"
        "from forze.application.contracts.querying.pagination.cursor_token import "
        "fingerprint_filter as f;"
        "print(f(P.parse({'$values': {'name': {'$in': ['a','b','c','d','e']}}})))"
    )

    def _run(seed: str) -> str:
        env = {**os.environ, "PYTHONHASHSEED": seed}
        out = subprocess.run(
            [sys.executable, "-c", prog],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        return out.stdout.strip()

    assert _run("0") == _run("1")


# ----------------------- #
# AEAD payload confidentiality: P3


def test_encrypted_cursor_round_trips_and_hides_the_payload() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        _b64url_decode,
        bind_cursor_cipher,
    )

    with bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)):
        tok = encode_keyset_v1(
            sort_keys=["score"], directions=["desc"], values=[4242]
        )
        # Encrypted tokens carry the scheme marker and are opaque — the ciphertext holds neither
        # the plaintext payload keys (``score``) nor the boundary value (``4242``), so a boundary
        # sort-key value that isn't in the row projection stays hidden.
        assert tok.startswith("~")
        body = _b64url_decode(tok[1:])
        assert b"score" not in body
        assert b"4242" not in body
        # Round-trips back to the exact values.
        assert validate_cursor_token(
            tok, sort_keys=["score"], directions=["desc"]
        ) == [4242]


def test_encrypted_cursor_uses_a_fresh_nonce_each_time() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        bind_cursor_cipher,
    )

    with bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)):
        a = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[1])
        b = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[1])
        # Same cursor, different ciphertext (random nonce) — but both decrypt identically.
        assert a != b
        assert validate_cursor_token(a, sort_keys=["id"], directions=["asc"]) == [1]
        assert validate_cursor_token(b, sort_keys=["id"], directions=["asc"]) == [1]


def test_encrypted_cursor_rejects_tampering_and_a_foreign_key() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        bind_cursor_cipher,
    )

    cipher = CursorTokenCipher(secret=b"z" * 32)

    with bind_cursor_cipher(cipher):
        tok = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[9])

        # Flip a ciphertext byte (index 6, well past the ``~`` marker) -> AEAD auth fails.
        i = 6
        flipped = tok[:i] + ("A" if tok[i] != "A" else "B") + tok[i + 1 :]
        with pytest.raises(CoreException):
            validate_cursor_token(flipped, sort_keys=["id"], directions=["asc"])

    # A token sealed under one key does not open under another.
    with bind_cursor_cipher(CursorTokenCipher(secret=b"w" * 32)):
        with pytest.raises(CoreException):
            validate_cursor_token(tok, sort_keys=["id"], directions=["asc"])


def test_encrypted_cursor_rejects_malformed_and_truncated_bodies() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        bind_cursor_cipher,
    )

    with bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)):
        # A ``~`` body that is not decodable base64url (one lone data char) -> validation.
        with pytest.raises(CoreException):
            decode_keyset_v1("~A")

        # A well-formed base64url body too short to hold even the nonce -> validation.
        with pytest.raises(CoreException):
            decode_keyset_v1("~AAAA")


def test_encryption_is_a_hard_cutover() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        _CODEC,
        CursorTokenCipher,
        bind_cursor_cipher,
    )

    # A previously-minted plaintext (or signed) token has no ``~`` marker, so once a cipher is
    # configured it is rejected outright — the client restarts pagination from page 1.
    plaintext = _CODEC.dumps(
        {"v": _KEYSET_V1, "k": ["id"], "d": ["asc"], "n": ["first"], "x": [5]}
    )

    with bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)):
        with pytest.raises(CoreException):
            decode_keyset_v1(plaintext)


def test_cipher_supersedes_signer_when_both_are_bound() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        CursorTokenSigner,
        bind_cursor_cipher,
        bind_cursor_signer,
    )

    with (
        bind_cursor_signer(CursorTokenSigner(secret=b"k" * 32)),
        bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)),
    ):
        tok = encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[3])
        # The cipher wins: the token is encrypted (not a signed ``payload.hmac``) and verifies.
        assert tok.startswith("~")
        assert "." not in tok
        assert validate_cursor_token(tok, sort_keys=["id"], directions=["asc"]) == [3]


def test_encrypted_cursor_still_enforces_context_binding() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        bind_cursor_cipher,
        build_cursor_binding,
    )

    f_open = _parse_filter({"$values": {"status": {"$eq": "open"}}})
    f_closed = _parse_filter({"$values": {"status": {"$eq": "closed"}}})
    minted = build_cursor_binding(spec_name="orders", tenant_id="t1", filter_expr=f_open)

    with bind_cursor_cipher(CursorTokenCipher(secret=b"z" * 32)):
        tok = encode_keyset_v1(
            sort_keys=["id"], directions=["asc"], values=[5], binding=minted
        )
        assert validate_cursor_token(
            tok, sort_keys=["id"], directions=["asc"], binding=minted
        ) == [5]

        # The binding rides *inside* the ciphertext now, and a replay under a different filter
        # is still rejected after decryption.
        other = build_cursor_binding(
            spec_name="orders", tenant_id="t1", filter_expr=f_closed
        )
        with pytest.raises(CoreException):
            validate_cursor_token(
                tok, sort_keys=["id"], directions=["asc"], binding=other
            )


def test_bind_cursor_cipher_scopes_and_configure_restores() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
        bind_cursor_cipher,
        configure_cursor_cipher,
        current_cursor_cipher,
        cursor_protection_active,
    )

    assert current_cursor_cipher() is None
    assert cursor_protection_active() is False

    cipher = CursorTokenCipher(secret=b"z" * 32)

    with bind_cursor_cipher(cipher):
        assert current_cursor_cipher() is cipher
        # Protection is active under a cipher alone (no signer needed).
        assert cursor_protection_active() is True
        assert encode_keyset_v1(
            sort_keys=["id"], directions=["asc"], values=[1]
        ).startswith("~")

    assert current_cursor_cipher() is None

    # configure_* set/restore mirrors the signer's.
    previous = configure_cursor_cipher(cipher)
    try:
        assert current_cursor_cipher() is cipher
    finally:
        configure_cursor_cipher(previous)
    assert current_cursor_cipher() is None


def test_short_cipher_secret_is_refused() -> None:
    from forze.application.contracts.querying.pagination.cursor_token import (
        CursorTokenCipher,
    )

    with pytest.raises(ValueError):
        CursorTokenCipher(secret=b"short")


def test_encode_keyset_misaligned_raises() -> None:
    with pytest.raises(CoreException, match="aligned"):
        encode_keyset_v1(sort_keys=["a"], directions=["asc", "asc"], values=[1])
    with pytest.raises(CoreException, match="aligned"):
        encode_keyset_v1(sort_keys=[], directions=[], values=[])


def test_decode_keyset_invalid_base64() -> None:
    with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
        decode_keyset_v1("not-valid-base64!!!")
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_decode_keyset_wrong_version() -> None:
    import base64
    import json

    raw = json.dumps({"v": 99, "k": ["a"], "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_decode_keyset_invalid_direction_in_payload() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": ["a"], "d": ["sideways"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_row_value_for_sort_key_nested() -> None:
    row = {"meta": {"inner": {"k": 42}}}
    assert row_value_for_sort_key(row, "meta.inner.k") == 42
    assert row_value_for_sort_key(row, "meta.missing.leaf") is None
    assert row_value_for_sort_key({"meta": "scalar"}, "meta.inner") is None


def test_compare_keyset_sort_values_uuid_and_string() -> None:
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert compare_keyset_sort_values(u, str(u)) == 0
    assert compare_keyset_sort_values(str(u), u) == 0


def test_row_passes_keyset_seek_uuid_after_desc() -> None:
    u1 = UUID("22222222-2222-2222-2222-222222222222")
    u2 = UUID("11111111-1111-1111-1111-111111111111")
    token = encode_keyset_v1(
        sort_keys=["id"],
        directions=["desc"],
        values=[u1],
    )
    _, _, _, cursor_vals = decode_keyset_v1(token)
    assert row_passes_keyset_seek(
        {"id": u2},
        sort_keys=["id"],
        directions=["desc"],
        cursor_values=cursor_vals,
        after=True,
    )
    assert not row_passes_keyset_seek(
        {"id": u1},
        sort_keys=["id"],
        directions=["desc"],
        cursor_values=cursor_vals,
        after=True,
    )


# ----------------------- #
# Shared keyset-cursor token-tail helpers


def test_validate_cursor_token_roundtrip_returns_values() -> None:
    sort_keys = ["created_at", "id"]
    directions = ["desc", "asc"]
    token = encode_keyset_v1(
        sort_keys=sort_keys, directions=directions, values=["2024-01-01", "abc"]
    )

    assert validate_cursor_token(
        token, sort_keys=sort_keys, directions=directions
    ) == ["2024-01-01", "abc"]


def test_validate_cursor_token_rejects_key_mismatch() -> None:
    token = encode_keyset_v1(sort_keys=["a"], directions=["asc"], values=[1])

    with pytest.raises(CoreException, match="Cursor does not match") as exc_info:
        validate_cursor_token(token, sort_keys=["b"], directions=["asc"])
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_validate_cursor_token_rejects_direction_mismatch() -> None:
    token = encode_keyset_v1(sort_keys=["a"], directions=["asc"], values=[1])

    with pytest.raises(CoreException, match="Cursor does not match") as exc_info:
        validate_cursor_token(token, sort_keys=["a"], directions=["desc"])
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def _rows(n: int) -> list[dict[str, int]]:
    return [{"id": i} for i in range(n)]


def test_keyset_page_bounds_after_trims_and_emits_next() -> None:
    # over-fetched limit+1 rows -> has_more, next cursor from last kept row, no prev on first page
    rows, has_more, nxt, prv = keyset_page_bounds(
        _rows(4), 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=False
    )
    assert [r["id"] for r in rows] == [0, 1, 2]
    assert has_more is True
    assert nxt is not None
    assert prv is None  # first page (no after/before) emits no prev


def test_keyset_page_bounds_after_page_emits_prev() -> None:
    _, has_more, nxt, prv = keyset_page_bounds(
        _rows(4), 3, sort_keys=["id"], directions=["asc"], use_after=True, use_before=False
    )
    assert has_more is True
    assert nxt is not None
    assert prv is not None  # an 'after' page can page back


def test_keyset_page_bounds_before_reverses_then_trims() -> None:
    # 'before' fetches in flipped order; the helper reverses then trims to the window.
    raw = [{"id": i} for i in (3, 2, 1, 0)]
    rows, has_more, _nxt, prv = keyset_page_bounds(
        raw, 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=True
    )
    assert [r["id"] for r in rows] == [0, 1, 2]  # reversed([3,2,1,0])[:3]
    assert has_more is True
    assert prv is not None  # paging 'before' with more remaining emits a prev cursor


def test_keyset_page_bounds_exact_fit_has_no_more() -> None:
    rows, has_more, nxt, _ = keyset_page_bounds(
        _rows(3), 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=False
    )
    assert [r["id"] for r in rows] == [0, 1, 2]
    assert has_more is False
    assert nxt is None


# ----------------------- #
# Canonicalization + comparison branch coverage


class _Weird:
    def __str__(self) -> str:
        return "weird"


@pytest.mark.parametrize(
    "value,expected",
    [
        ([1, 2], [1, 2]),  # list passthrough
        ({"a": 1}, {"a": 1}),  # dict passthrough
        (_Weird(), "weird"),  # fallback str()
        ("s", "s"),
        (3, 3),
    ],
)
def test_keyset_canonical_value(value: object, expected: object) -> None:
    assert keyset_canonical_value(value) == expected


@pytest.mark.parametrize(
    "left,right,expected",
    [
        (None, 1, -1),  # lc is None
        (1, None, 1),  # rc is None
        (1, 1, 0),  # equal
        (1, 2, -1),  # lc < rc
        (2, 1, 1),  # lc > rc
        (None, None, 0),  # both None -> equal
    ],
)
def test_compare_keyset_sort_values(left: object, right: object, expected: int) -> None:
    assert compare_keyset_sort_values(left, right) == expected


def test_decode_keyset_rejects_container_values() -> None:
    # Tampered token: client-controlled values must be JSON scalars only.
    import base64
    import json

    for bad in ({"a": 1}, [1, 2]):
        raw = json.dumps({"v": 1, "k": ["a"], "d": ["asc"], "x": [bad]}).encode()
        token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
            decode_keyset_v1(token)
        assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_compare_keyset_mixed_types_raises_validation_not_type_error() -> None:
    # A tampered cursor can put an int next to a str row value; that must surface
    # as an invalid-cursor validation error, never a raw TypeError (500).
    with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
        compare_keyset_sort_values(1, "abc")
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_well_formed_token_round_trips_unchanged() -> None:
    sort_keys = ["name", ID_FIELD]
    directions = ["asc", "asc"]
    values = ["alice", "a1"]
    token = encode_keyset_v1(
        sort_keys=sort_keys, directions=directions, values=values
    )

    assert (
        validate_cursor_token(token, sort_keys=sort_keys, directions=directions)
        == values
    )


def test_pre_codec_fixed_token_decodes_and_reencodes_identically() -> None:
    # Token hard-coded from the hand-rolled json/base64 implementation that
    # predates the :class:`~forze.base.codecs.B64UrlJsonCodec` swap. In-flight
    # cursors must keep decoding, and re-encoding the same payload must produce
    # the identical token bytes (wire compatibility in both directions).
    token = (
        "eyJkIjpbImRlc2MiLCJkZXNjIiwiZGVzYyJdLCJrIjpbImNyZWF0ZWRfYXQiLCJuYW1l"
        "IiwiaWQiXSwidiI6MSwieCI6WyIyMDI2LTAxLTAyVDAzOjA0OjA1IiwiYWxpY2UiLCIw"
        "MTkzZTRjMi1hYWFhLWJiYmItY2NjYy0xMjM0NTY3ODkwYWIiXX0"
    )

    keys, dirs, nulls, vals = decode_keyset_v1(token)

    assert keys == ["created_at", "name", "id"]
    assert dirs == ["desc", "desc", "desc"]
    # A pre-null-placement token carries no ``n`` field; nulls default to canonical.
    assert nulls == ["last", "last", "last"]
    assert vals == [
        "2026-01-02T03:04:05",
        "alice",
        "0193e4c2-aaaa-bbbb-cccc-1234567890ab",
    ]
    # Re-encoding now adds the ``n`` field, so it isn't byte-identical to the old token;
    # it must round-trip to the same logical payload, including the defaulted nulls.
    assert decode_keyset_v1(
        encode_keyset_v1(sort_keys=keys, directions=dirs, nulls=nulls, values=vals)
    ) == (keys, dirs, nulls, vals)


def test_pre_codec_token_with_escaped_unicode_still_decodes() -> None:
    # The old encoder escaped non-ASCII as \uXXXX (ensure_ascii=True); such
    # tokens must keep decoding to the same values.
    token = "eyJkIjpbImFzYyJdLCJrIjpbIm5hbWUiXSwidiI6MSwieCI6WyJoXHUwMGU5bGxvIl19"

    keys, dirs, nulls, vals = decode_keyset_v1(token)

    assert keys == ["name"]
    assert dirs == ["asc"]
    assert nulls == ["first"]
    assert vals == ["héllo"]


def test_non_ascii_value_round_trips() -> None:
    token = encode_keyset_v1(sort_keys=["name"], directions=["asc"], values=["héllo"])

    assert decode_keyset_v1(token) == (["name"], ["asc"], ["first"], ["héllo"])


def test_decode_keyset_rejects_non_list_payload() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": "notlist", "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_decode_keyset_rejects_length_mismatch() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": ["a", "b"], "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def _tampered_token(payload: dict) -> str:
    """Encode a raw payload directly (bypassing the aligned-by-construction encoder)."""
    return _CODEC.dumps({"v": _KEYSET_V1, **payload})


def test_ordered_compare_type_mismatch_is_invalid_cursor() -> None:
    # A tampered token can pair a value of the wrong type against a row value; the raw
    # TypeError is surfaced as a clean invalid-cursor error.
    with pytest.raises(CoreException, match="Invalid cursor token") as ei:
        ordered_compare(1, "x", direction="asc", nulls="first")

    assert ei.value.kind is ExceptionKind.VALIDATION


def test_decode_rejects_misaligned_nulls_array() -> None:
    token = _tampered_token(
        {"k": ["a"], "d": ["asc"], "x": [1], "n": ["first", "last"]},
    )
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_decode_rejects_invalid_nulls_placement_value() -> None:
    token = _tampered_token({"k": ["a"], "d": ["asc"], "x": [1], "n": ["middle"]})
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)
