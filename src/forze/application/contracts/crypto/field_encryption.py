"""Field-encryption policy: which record fields are sealed at rest, and how."""

from collections.abc import Iterable
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


def _field_set(value: str | Iterable[str]) -> frozenset[str]:
    """Coerce field names to a frozenset, treating a bare ``str`` as one field name.

    ``frozenset("email")`` would silently iterate the characters (``{'e','m',...}``), leaving
    the intended field unencrypted; a single field name passed as a plain string is the common
    mistake, so it is wrapped into a one-element set instead.
    """

    if isinstance(value, str):
        return frozenset({value})

    return frozenset(value)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FieldEncryption:
    """Which fields of a record are sealed at rest, and how — one declaration shared by
    every spec (document, search, hub, ...) that reads or writes the same data.

    Encryption is a *persistence* concern, not a domain one: the field names live here, on
    an application-layer policy object, never on the domain or read model. A ``DocumentSpec``
    and the ``SearchSpec`` over the same table point at the *same* policy, so their
    encrypted / searchable sets and record-id binding cannot drift out of sync.
    """

    encrypted: frozenset[str] = attrs.field(factory=frozenset, converter=_field_set)
    """Stored field names to encrypt at rest with **randomized** field-level encryption.

    Confidential: decrypted on full-model reads and on typed (``select_*``) / raw
    (``project_*``) projections that select them, but *not* filterable or sortable. Requires
    a ``KeyringDepKey`` in the deps (e.g. via ``CryptoDepsModule``)."""

    searchable: frozenset[str] = attrs.field(factory=frozenset, converter=_field_set)
    """Stored field names to encrypt **deterministically** so equality queries still work.

    Same plaintext → same ciphertext, so ``$eq``/``$neq``/``$in``/``$nin`` filters are
    transparently rewritten to match the value at rest — no separate blind-index column. The
    trade: deterministic encryption leaks equality/frequency within a tenant, and only
    equality (not range/sort/like) is supported. Disjoint from :attr:`encrypted`. Requires a
    ``DeterministicCipherDepKey`` in the deps."""

    binds_record_id: bool = False
    """Bind the record's ``id`` into the AAD of every :attr:`encrypted` ciphertext.

    Defaults to ``False`` (the AAD already binds field name + tenant). When ``True``, a
    ciphertext additionally cannot be transplanted to a *different record of the same tenant
    and field*. Applies only to randomized :attr:`encrypted` fields (never to
    :attr:`searchable`, whose ciphertext must stay record-independent for equality queries).
    Consequence: a filter-based bulk ``update_matching`` of a bound field is refused (no
    per-record id), and ciphertext written before enabling this still decrypts only without
    the binding."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if overlap := self.encrypted & self.searchable:
            raise exc.configuration(
                f"FieldEncryption declares fields {sorted(overlap)} as both encrypted "
                "(randomized) and searchable (deterministic); the sets must be disjoint.",
            )

    # ....................... #

    @property
    def is_empty(self) -> bool:
        """No fields are sealed — nothing to encrypt or decrypt."""

        return not (self.encrypted or self.searchable)

    # ....................... #

    def validate_fields_exist(
        self, stored_fields: frozenset[str], *, spec_name: object
    ) -> None:
        """Reject a sealed field name absent from *stored_fields* (a typo footgun).

        A misspelt encrypted/searchable field silently encrypts nothing — the intended column
        stays plaintext at rest — so unknown names are rejected at spec construction against the
        model's stored field names.
        """

        if unknown := sorted((self.encrypted | self.searchable) - stored_fields):
            raise exc.configuration(
                f"FieldEncryption for {spec_name!r} names field(s) {unknown} that are not "
                f"stored fields of the read model {sorted(stored_fields)}. Check for typos."
            )

    # ....................... #

    def forbidden_sort_fields(self, fields: Iterable[str]) -> list[str]:
        """Of *fields*, the sealed ones — which have no order at rest and can't be sort keys.

        A randomized :attr:`encrypted` ciphertext is unordered and a deterministic
        :attr:`searchable` one supports equality only; sorting on either is meaningless and a
        keyset cursor would leak its raw value in the token.

        Sealing applies to whole top-level columns, so a *nested* sort key is forbidden when
        its **root** segment is sealed: sorting on ``contract.ssn`` is rejected when
        ``contract`` is encrypted (the value still lives inside the sealed ciphertext).
        """

        sealed = self.encrypted | self.searchable
        return sorted({field for field in fields if field.split(".", 1)[0] in sealed})
