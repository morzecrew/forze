"""Field-level encrypting ``ModelCodec`` decorator.

Wraps any :class:`~forze.base.serialization.model_codec.ModelCodec` and transparently
encrypts a configured set of fields on the **persistence** path while leaving the
rest plaintext so the backend can still index, route, and query them. This is the
canonical shape for at-rest field encryption in a document/SQL store.

The codec is synchronous (the ``ModelCodec`` protocol is), so it cannot call the
async key backend inline. It relies on an async pre-pass priming the keyring:

- :meth:`prepare_encrypt` (``warm``) before a synchronous encode, and
- :meth:`prepare_decrypt` (``ensure_unwrapped``) before a synchronous decode.

A gateway runs these in its async read/write methods around the sync codec call;
a same-process read-after-write needs no decrypt pre-pass (the cache is already
seeded). Encryption applies only to ``encode_persistence_mapping`` /
``decode_mapping`` (the DB path) — event/JSON serialization passes through, since
whole-message protection is the outbox/transport concern, not this one.

Encrypted field values are stored as base64 strings of a self-describing
envelope, so the columns must accept text (a JSON/JSONB field does); a value that
is not an envelope is passed through untouched, tolerating legacy plaintext.
"""

import base64
import binascii
from collections.abc import Callable
from typing import Any, Iterator, Literal, Sequence, final

import attrs
import orjson

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
)
from forze.application.contracts.document import DocumentCodecs
from forze.application.contracts.querying import (
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import is_envelope, unpack_envelope
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict
from forze.base.serialization.model_codec import ModelCodec, ModelDumpExcludeOptions

# ----------------------- #

_MISSING = object()
_UNSET = object()

_AEAD_AUTH_FAILED_CODE = "core.crypto.aead_auth_failed"
"""Code raised by the AEAD when a tag check fails (see ``forze.base.crypto.ciphers``).
The decrypt path treats *only* this as a signal to retry with the legacy (pre-record-id)
AAD during a binding migration — any other failure (e.g. ``cipher_not_warm``) propagates."""


def _maybe_envelope(value: str) -> bytes | None:
    """Return the envelope bytes if *value* is base64 of a Forze envelope, else ``None``."""

    try:
        blob = base64.b64decode(value, validate=True)

    except (binascii.Error, ValueError):
        return None

    return blob if is_envelope(blob) else None


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptingModelCodec[T](ModelCodec[T, Any]):
    """A ``ModelCodec`` that encrypts selected fields on the persistence path."""

    inner: ModelCodec[T, Any]
    """The wrapped codec doing the real (de)serialization."""

    cipher: FieldCipherPort
    """Keyring providing the sync fast path and async pre-pass."""

    fields: frozenset[str]
    """Randomized-encrypted field names (not queryable, no equality leak)."""

    tenant_provider: Callable[[], TenantIdentity | None]
    """Returns the active tenant (typically ``ctx.inv_ctx.get_tenant``)."""

    searchable_fields: frozenset[str] = frozenset()
    """Deterministically-encrypted field names: equality-queryable (the filter is
    rewritten to match the ciphertext) at the cost of leaking equality within a
    tenant. Disjoint from :attr:`fields`."""

    deterministic: DeterministicFieldCipherPort | None = None
    """Cipher for :attr:`searchable_fields`; required when that set is non-empty."""

    label: str = "forze.field"
    """Associated-data namespace, so ciphertext cannot move between aggregates."""

    record_id_field: str | None = None
    """When set (e.g. ``"id"``), the record's value at this field is bound into the
    AAD of every randomized :attr:`fields` ciphertext, so a ciphertext cannot be
    transplanted to a different record (same tenant, same field). Off by default.
    Never applied to :attr:`searchable_fields` — deterministic ciphertext must stay
    record-independent for equality queries to match across rows."""

    # ....................... #

    @property
    def model_type(self) -> type[T]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self.inner.model_type

    # ....................... #
    # async pre-pass (called by the gateway around the sync codec)

    async def prepare_encrypt(self) -> None:
        """Warm the active data key so a subsequent sync encode does not block."""

        if self.fields:
            await self.cipher.warm(self.tenant_provider())

    async def prepare_decrypt(self, mappings: Sequence[JsonDict]) -> None:
        """Unwrap the data keys named by *mappings*' encrypted fields for sync decode."""

        if not self.fields:
            return

        envelopes = [
            unpack_envelope(blob)
            for mapping in mappings
            for field in self.fields
            if isinstance((value := mapping.get(field)), str)
            and (blob := _maybe_envelope(value)) is not None
        ]

        if envelopes:
            await self.cipher.ensure_unwrapped(envelopes)

    # ....................... #
    # field crypto helpers

    def _aad(
        self,
        field: str,
        tenant: TenantIdentity | None,
        *,
        record_id: str | None = None,
    ) -> bytes:
        tenant_id = None if tenant is None else tenant.tenant_id
        base = f"{self.label}|field={field}|tenant={tenant_id}"

        if record_id is not None:
            base += f"|id={record_id}"

        return base.encode("utf-8")

    def _resolve_record_id(self, mapping: JsonDict, record_id: Any) -> str | None:
        """The record id to bind into AAD, or ``None`` when binding is off.

        Prefers an explicit *record_id* (the patch path threads the target pk, since
        a partial update DTO has none) and falls back to the value in *mapping* (the
        full-document and read paths carry it). Raises when binding is on but no id is
        available — e.g. a filter-based bulk update of an id-bound encrypted field,
        which cannot supply a per-record id.
        """

        if self.record_id_field is None:
            return None

        rid = record_id if record_id is not None else mapping.get(self.record_id_field)

        if rid is None:
            raise exc.precondition(
                f"Encrypted field bound to record id cannot be written without one "
                f"({self.record_id_field!r} absent). A filter-based bulk update of an "
                "id-bound encrypted field is unsupported — update records individually.",
                code="core.crypto.record_id_required",
            )

        return str(rid)

    def _require_det(self) -> DeterministicFieldCipherPort:
        if self.deterministic is None:
            raise exc.internal(
                "Searchable fields require a deterministic cipher; none is wired.",
                code="core.crypto.deterministic_missing",
            )

        return self.deterministic

    def _det_encode(
        self,
        tenant: TenantIdentity | None,
        field: str,
        value: Any,
    ) -> str:
        """Deterministic ciphertext (base64) for a value — stable across calls."""

        blob = self._require_det().encrypt(
            tenant=tenant,
            field=field,
            plaintext=orjson.dumps(value),
        )
        return base64.b64encode(blob).decode("ascii")

    def _encrypt_fields(self, mapping: JsonDict, *, record_id: Any = None) -> JsonDict:
        if not self.fields and not self.searchable_fields:
            return mapping

        tenant = self.tenant_provider()
        out = dict(mapping)

        # Resolve the bound id lazily — only when a field is actually encrypted — so a
        # bulk update touching no encrypted field never trips the no-id guard.
        bound_id: Any = _UNSET

        for field in self.fields:
            value = out.get(field, _MISSING)

            if value is _MISSING or value is None:
                continue

            if bound_id is _UNSET:
                bound_id = self._resolve_record_id(mapping, record_id)

            blob = self.cipher.encrypt_sync(
                orjson.dumps(value),
                tenant=tenant,
                aad=self._aad(field, tenant, record_id=bound_id),
            )
            out[field] = base64.b64encode(blob).decode("ascii")

        for field in self.searchable_fields:
            value = out.get(field, _MISSING)

            if value is _MISSING or value is None:
                continue

            out[field] = self._det_encode(tenant, field, value)

        return out

    def encrypt_mapping(self, mapping: JsonDict, *, record_id: Any = None) -> JsonDict:
        """Encrypt the encrypted/searchable fields in a raw property map.

        Public entry for adapters that seal a field-mapping directly rather than a model — e.g.
        graph node/edge properties built from a create/update DTO (distinct from the read
        model). Requires the active data key to be warmed (:meth:`prepare_encrypt`). *record_id*
        binds the row id into the AAD when ``record_id_field`` is set. A no-op when no fields are
        marked for encryption.
        """

        return self._encrypt_fields(mapping, record_id=record_id)

    def decrypt_mapping(self, mapping: JsonDict) -> JsonDict:
        """Decrypt the encrypted/searchable fields in a raw stored row.

        Public entry for the read gateway's projection path: a projection decodes
        with a *different* (plaintext) codec, so the gateway pre-decrypts the raw
        row with this before that decode. Requires the data keys to be warmed
        (the gateway runs :meth:`prepare_decrypt` first). A no-op on rows whose
        marked fields are already plaintext (migration tolerance).
        """

        return self._decrypt_fields(mapping)

    def _decrypt_fields(self, mapping: JsonDict) -> JsonDict:
        if not self.fields and not self.searchable_fields:
            return mapping

        out: JsonDict | None = None
        tenant: Any = _UNSET

        for field in self.fields:
            value = mapping.get(field)

            if not isinstance(value, str):
                continue

            blob = _maybe_envelope(value)

            if blob is None:
                continue

            if tenant is _UNSET:
                tenant = self.tenant_provider()

            if out is None:
                out = dict(mapping)

            out[field] = orjson.loads(
                self._decrypt_field_blob(field, tenant, mapping, blob)
            )

        if self.searchable_fields:
            det = self._require_det()

            for field in self.searchable_fields:
                value = mapping.get(field)

                if not isinstance(value, str):
                    continue

                try:
                    ciphertext = base64.b64decode(value, validate=True)

                except (binascii.Error, ValueError):
                    continue  # legacy plaintext

                if tenant is _UNSET:
                    tenant = self.tenant_provider()

                try:
                    raw = det.decrypt(
                        tenant=tenant,
                        field=field,
                        ciphertext=ciphertext,
                    )

                except CoreException:
                    continue  # valid base64 but not our ciphertext → legacy plaintext

                if out is None:
                    out = dict(mapping)

                out[field] = orjson.loads(raw)

        return out if out is not None else mapping

    def _decrypt_field_blob(
        self,
        field: str,
        tenant: TenantIdentity | None,
        mapping: JsonDict,
        blob: bytes,
    ) -> bytes:
        """Decrypt one randomized-field envelope, tolerating a binding migration.

        With record-id binding on, try the id-bound AAD first; on an AEAD auth
        failure (only) retry with the legacy AAD, so ciphertext written before
        binding was enabled still reads. A transplanted id-bound ciphertext fails
        both AADs and stays rejected; any non-auth failure propagates unchanged.
        """

        if self.record_id_field is None:
            return self.cipher.decrypt_sync(blob, aad=self._aad(field, tenant))

        rid = mapping.get(self.record_id_field)

        if rid is not None:
            try:
                return self.cipher.decrypt_sync(
                    blob, aad=self._aad(field, tenant, record_id=str(rid))
                )

            except CoreException as error:
                if error.code != _AEAD_AUTH_FAILED_CODE:
                    raise
                # Pre-binding ciphertext: retry the legacy id-less AAD. A transplanted
                # id-bound value fails this too and stays rejected (aead_auth_failed).
                return self.cipher.decrypt_sync(blob, aad=self._aad(field, tenant))

        # Binding is configured but the record id is absent from this row. A legacy
        # (pre-binding) value still reads id-less; an id-bound value fails — surface the
        # missing id as a clear misconfiguration rather than an opaque tamper error
        # (e.g. a projection that selected the encrypted field but not its id column).
        try:
            return self.cipher.decrypt_sync(blob, aad=self._aad(field, tenant))

        except CoreException as error:
            if error.code != _AEAD_AUTH_FAILED_CODE:
                raise
            raise exc.precondition(
                f"Cannot decrypt id-bound field {field!r}: its record id field "
                f"({self.record_id_field!r}) is absent from the row — include it in the "
                "projection (or the value predates record-id binding).",
                code="core.crypto.record_id_required",
            ) from error

    # ....................... #
    # query rewrite (deterministic searchable fields)

    def rewrite_filter(self, expr: QueryExpr) -> QueryExpr:
        """Rewrite equality predicates on searchable fields to match the ciphertext.

        Called by the gateway's filter compilation. Recurses through and/or/not and
        replaces the literal in an ``$eq``/``$neq``/``$in``/``$nin`` predicate on a
        searchable field with its deterministic ciphertext; rejects any other
        operator on a searchable field (deterministic encryption supports equality
        only).
        """

        if not self.searchable_fields:
            return expr

        return self._rewrite_node(expr, self.tenant_provider())

    def _rewrite_node(self, node: QueryExpr, tenant: TenantIdentity | None) -> QueryExpr:
        match node:
            case QueryAnd(items):
                return QueryAnd(tuple(self._rewrite_node(i, tenant) for i in items))

            case QueryOr(items):
                return QueryOr(tuple(self._rewrite_node(i, tenant) for i in items))

            case QueryNot(item):
                return QueryNot(self._rewrite_node(item, tenant))

            case QueryField(name, op, value) if name in self.searchable_fields:
                return self._rewrite_field(name, op, value, tenant)

            case QueryCompare(left, _op, right) if (
                left in self.searchable_fields or right in self.searchable_fields
            ):
                raise exc.precondition(
                    "Field-to-field comparison is not supported on an encrypted "
                    f"searchable field ({left!r} vs {right!r}); only equality against "
                    "a literal.",
                    code="core.crypto.searchable_op_unsupported",
                )

            case QueryElem(path, quantifier, inner):
                if path in self.searchable_fields:
                    raise exc.precondition(
                        f"Array-element quantifiers are not supported on encrypted "
                        f"searchable field {path!r}.",
                        code="core.crypto.searchable_op_unsupported",
                    )

                # Recurse the element predicate to catch (and reject) any searchable
                # field referenced inside it.
                return QueryElem(path, quantifier, self._rewrite_node(inner, tenant))

            case _:
                return node

    def _det_search(self, tenant: TenantIdentity | None, field: str, value: Any) -> tuple[str, ...]:
        """Base64 ciphertext(s) a stored value could match — both keys during rotation."""

        return tuple(
            base64.b64encode(blob).decode("ascii")
            for blob in self._require_det().search_variants(
                tenant=tenant, field=field, plaintext=orjson.dumps(value)
            )
        )

    def _rewrite_field(
        self,
        name: str,
        op: Any,
        value: Any,
        tenant: TenantIdentity | None,
    ) -> QueryField:
        if op in ("$eq", "$neq"):
            variants = self._det_search(tenant, name, value)

            # Steady state → one ciphertext, keep the equality op. During a rotation
            # overlap a value may sit under either key, so widen to membership so the
            # predicate matches both: $eq → $in, $neq → $nin.
            if len(variants) == 1:
                return QueryField(name, op, variants[0])

            return QueryField(name, "$in" if op == "$eq" else "$nin", variants)

        if op in ("$in", "$nin"):
            return QueryField(
                name,
                op,
                tuple(ct for v in value for ct in self._det_search(tenant, name, v)),
            )

        raise exc.precondition(
            f"Operator {op!r} is not supported on encrypted searchable field "
            f"{name!r}; only equality ($eq/$neq) and membership ($in/$nin).",
            code="core.crypto.searchable_op_unsupported",
        )

    # ....................... #
    # persistence path — encrypts / decrypts

    def encode_persistence_mapping(
        self,
        obj: T,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict:
        return self._encrypt_fields(
            self.inner.encode_persistence_mapping(obj, mode=mode, exclude=exclude)
        )

    def encode_persistence_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        return [
            self._encrypt_fields(m)
            for m in self.inner.encode_persistence_mapping_many(
                objs, mode=mode, exclude=exclude
            )
        ]

    def encode_persistence_patch(
        self,
        obj: T,
        *,
        record_id: Any = None,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict:
        """Encode an update DTO, binding *record_id* into encrypted-field AAD.

        The patch-path analog of :meth:`encode_persistence_mapping`: a partial update
        DTO carries no record id, so the gateway threads the target ``pk`` here. With
        record-id binding off (the default) this is equivalent to
        ``encode_persistence_mapping`` — *record_id* is ignored. The write gateways
        discover this method by duck typing, so plain codecs need not define it.
        """

        return self._encrypt_fields(
            self.inner.encode_persistence_mapping(obj, mode=mode, exclude=exclude),
            record_id=record_id,
        )

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> T:
        return self.inner.decode_mapping(
            self._decrypt_fields(data),
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> list[T]:
        return self.inner.decode_mapping_many(
            [self._decrypt_fields(d) for d in data],
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> Iterator[list[T]]:
        # Decrypt one batch at a time so peak memory stays at ~``batch_size`` decrypted
        # rows — decrypting all of ``data`` upfront would defeat the caller's batching.
        for start in range(0, len(data), batch_size):
            chunk = data[start : start + batch_size]
            yield self.inner.decode_mapping_many(
                [self._decrypt_fields(d) for d in chunk],
                forbid_extra=forbid_extra,
                trust_source=trust_source,
            )

    # ....................... #
    # passthrough — no field crypto (events / JSON / transforms / introspection)

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict:
        return self.inner.encode_mapping(obj, mode=mode, exclude=exclude)

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        return self.inner.encode_mapping_many(objs, mode=mode, exclude=exclude)

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> Iterator[list[JsonDict]]:
        return self.inner.encode_mapping_many_batched(
            objs, batch_size=batch_size, mode=mode, exclude=exclude
        )

    def transform(
        self,
        source: Any,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {"unset": True},
    ) -> T:
        return self.inner.transform(source, mode=mode, exclude=exclude)

    def transform_many(
        self,
        sources: Sequence[Any],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions = {"unset": True},
    ) -> list[T]:
        return self.inner.transform_many(sources, mode=mode, exclude=exclude)

    def stored_field_names(self, *, include_computed: bool = True) -> frozenset[str]:
        return self.inner.stored_field_names(include_computed=include_computed)

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: ModelDumpExcludeOptions = {},
    ) -> bytes:
        return self.inner.encode_json_bytes(obj, exclude=exclude)

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T:
        return self.inner.decode_json_bytes(
            raw, forbid_extra=forbid_extra, encoding=encoding
        )


# ....................... #


def encrypting_document_codecs(
    codecs: DocumentCodecs[Any, Any, Any, Any],
    *,
    fields: frozenset[str],
    cipher: FieldCipherPort,
    tenant_provider: Callable[[], TenantIdentity | None],
    label: str,
    searchable_fields: frozenset[str] = frozenset(),
    deterministic: DeterministicFieldCipherPort | None = None,
    record_id_field: str | None = None,
) -> DocumentCodecs[Any, Any, Any, Any]:
    """Wrap a document codec bundle so fields are encrypted on the persistence path.

    *fields* are randomized-encrypted; *searchable_fields* are deterministically
    encrypted (equality-queryable). The ``read`` (decrypt-on-read), ``domain``
    (encrypt-on-write) and ``update`` (encrypt-on-patch) codecs are wrapped. The
    ``create`` codec is left untouched — it only transforms create commands into
    domain models, which is then encrypted via ``domain``. ``history`` is left
    plaintext (encrypted-field history is not supported).

    *record_id_field* (e.g. ``"id"``) opts the randomized *fields* into record-id AAD
    binding (transplant resistance); ``None`` keeps the legacy unbound AAD.
    """

    def _wrap(inner: ModelCodec[Any, Any]) -> EncryptingModelCodec[Any]:
        return EncryptingModelCodec(
            inner=inner,
            cipher=cipher,
            fields=fields,
            tenant_provider=tenant_provider,
            searchable_fields=searchable_fields,
            deterministic=deterministic,
            label=label,
            record_id_field=record_id_field,
        )

    return DocumentCodecs(
        read=_wrap(codecs.read),
        domain=_wrap(codecs.domain) if codecs.domain is not None else None,
        create=codecs.create,
        update=_wrap(codecs.update) if codecs.update is not None else None,
        history=codecs.history,
    )
