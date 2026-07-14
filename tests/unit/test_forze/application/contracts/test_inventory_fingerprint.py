"""Unit tests for the spec inventory's structural fingerprint.

# covers: forze.application.contracts.inventory.FrozenSpecRegistry.fingerprint
# covers: forze.application.contracts.inventory.FrozenSpecRegistry.spec_fingerprint
# covers: forze.application.contracts.inventory.entry_shape

The fingerprint exists to be compared **between two processes** — an artifact's manifest against
the application about to import it. Every hazard it has is therefore invisible to a test that
computes it once, in one interpreter: a set of field names renders in hash order and string
hashing is seeded per process; a live object hashed through ``default=str`` renders its memory
address. Both are perfectly stable within a single run. So the load-bearing test here is
:func:`test_fingerprint_is_identical_across_processes`, which actually spawns them.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import BaseModel, create_model

from forze.application.contracts.analytics import (
    AnalyticsProvenance,
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.document import DocumentCodecs, DocumentSpec
from forze.application.contracts.inventory import (
    SpecEdgeKind,
    SpecPlane,
    SpecRegistry,
    SpecSource,
)
from forze.application.contracts.inventory.fingerprint import _shape
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec

# ----------------------- #


class _Model(BaseModel):
    id: str


class _Wide(BaseModel):
    """Five defaulted fields, so a ``frozenset`` of their names has an order to get wrong."""

    id: str
    alpha: str = "a"
    bravo: str = "b"
    charlie: str = "c"
    delta: str = "d"
    echo: str = "e"


def _document(name: str, **kwargs: object) -> DocumentSpec[_Model, _Model, _Model, _Model]:
    return DocumentSpec(
        name=name,
        read=_Model,
        write={"domain": _Model, "create_cmd": _Model, "update_cmd": _Model},
        **kwargs,  # type: ignore[arg-type]
    )


def _search(name: str) -> SearchSpec[_Model]:
    return SearchSpec(name=name, model_type=_Model, fields=["id"])


# ....................... #


def test_fingerprint_is_stable_and_order_independent() -> None:
    """The same application fingerprints the same, however its specs were registered."""

    forward = SpecRegistry().register(_document("a"), _document("b"), _search("s")).freeze()
    reverse = SpecRegistry().register(_search("s"), _document("b"), _document("a")).freeze()

    assert forward.fingerprint() == reverse.fingerprint()
    assert forward.fingerprint() == forward.fingerprint()


# ....................... #


def test_fingerprint_is_identical_across_processes() -> None:
    """The one test that can fail for a real reason — see the module docstring.

    A registry holding multi-element field sets, a codec, and the framework's own 19 identity
    specs must hash to the same bytes under any string-hash seed. Drop the sort in ``_shape``'s
    set branch and every seed below produces a different digest.
    """

    script = textwrap.dedent(
        """
        from pydantic import BaseModel

        from forze.application.contracts.document import DocumentSpec
        from forze.application.contracts.outbox import OutboxSpec
        from forze.base.serialization import PydanticModelCodec
        from forze_identity import spec_contributions


        class Wide(BaseModel):
            id: str
            alpha: str = "a"
            bravo: str = "b"
            charlie: str = "c"
            delta: str = "d"
            echo: str = "e"


        registry = spec_contributions()
        registry.register(
            DocumentSpec(
                name="wide",
                read=Wide,
                lenient_read_fields=frozenset({"alpha", "bravo", "charlie", "delta", "echo"}),
            ),
            OutboxSpec(name="events", codec=PydanticModelCodec(model_type=Wide)),
        )

        print(registry.freeze().fingerprint())
        """
    )

    digests = {
        seed: subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for seed in ("0", "1", "12345", "99999")
    }

    assert len(set(digests.values())) == 1, f"fingerprint varies by hash seed: {digests}"


# ....................... #


def test_a_model_is_hashed_by_its_schema_not_its_name() -> None:
    """Two models of the same name and different fields must not fingerprint alike.

    Hashing a model class by ``__qualname__`` would be deterministic *and* useless: the whole
    point is to notice that a read model's fields moved.
    """

    thin = create_model("Same", id=(str, ...))
    thick = create_model("Same", id=(str, ...), added=(int, ...))

    thin_fp = SpecRegistry().register(DocumentSpec(name="d", read=thin)).freeze().fingerprint()
    thick_fp = SpecRegistry().register(DocumentSpec(name="d", read=thick)).freeze().fingerprint()

    assert thin_fp != thick_fp


# ....................... #


def test_equal_specs_fingerprint_alike_despite_an_overridden_codec() -> None:
    """``a == b`` implies one fingerprint — the rule the registry already dedupes on.

    ``DocumentSpec.codecs`` is ``eq=False``: a codec override changes *how* a model is encoded,
    never *which* model, so attrs declares two specs differing only in it to be the same spec —
    and ``SpecRegistry.register_entry`` already dedupes them on exactly that. The fingerprint
    has to agree, or a registry would hash differently depending on whether an author had
    spelled out a codec the framework would have derived anyway.
    """

    bare = _document("orders")
    overridden = _document("orders", codecs=DocumentCodecs(read=PydanticModelCodec(_Model)))

    assert bare == overridden

    bare_fp = SpecRegistry().register(bare).freeze().fingerprint()
    overridden_fp = SpecRegistry().register(overridden).freeze().fingerprint()

    assert bare_fp == overridden_fp


# ....................... #


def test_a_codecs_materialized_set_reaches_the_fingerprint() -> None:
    """A codec is walked, not skipped: its model *and* its materialized set are portable shape."""

    plain = OutboxSpec(name="events", codec=PydanticModelCodec(model_type=_Model))
    other = OutboxSpec(name="events", codec=PydanticModelCodec(model_type=_Wide))

    plain_fp = SpecRegistry().register(plain).freeze().fingerprint()
    other_fp = SpecRegistry().register(other).freeze().fingerprint()

    assert plain_fp != other_fp


# ....................... #


def test_disposition_is_part_of_the_shape() -> None:
    """The same table, projected or system-of-record, is a different artifact."""

    queries = {"top": AnalyticsQueryDefinition(params=_Model)}

    projected = AnalyticsSpec(
        name="revenue", read=_Model, queries=queries, provenance=AnalyticsProvenance.PROJECTED
    )
    of_record = AnalyticsSpec(
        name="revenue",
        read=_Model,
        queries=queries,
        provenance=AnalyticsProvenance.SYSTEM_OF_RECORD,
    )

    projected_fp = SpecRegistry().register(projected).freeze().fingerprint()
    of_record_fp = SpecRegistry().register(of_record).freeze().fingerprint()

    assert projected_fp != of_record_fp


# ....................... #


def test_source_is_not_part_of_the_shape() -> None:
    """Moving a registration from an app into a kit changes nothing an import can observe."""

    authored = SpecRegistry().register(_document("orders"), source=SpecSource.AUTHOR).freeze()
    from_kit = SpecRegistry().register(_document("orders"), source=SpecSource.KIT).freeze()

    assert authored.fingerprint() == from_kit.fingerprint()


# ....................... #


def test_a_lost_edge_changes_the_fingerprint() -> None:
    """A ``REBUILDS_FROM`` edge is what makes an import reindex a search plane. Losing it is
    silent at runtime and must not be silent here."""

    document, search = _document("orders"), _search("orders_idx")

    without = SpecRegistry().register(document, search).freeze()
    with_edge = (
        SpecRegistry()
        .register(document, search)
        .link(SpecEdgeKind.REBUILDS_FROM, source=search, target=document)
        .freeze()
    )

    assert without.fingerprint() != with_edge.fingerprint()


# ....................... #


def test_a_field_set_changes_the_fingerprint() -> None:
    """A frozenset field is real shape, not decoration — the set branch must actually hash it."""

    lean = DocumentSpec(name="orders", read=_Wide, lenient_read_fields=frozenset())
    rich = DocumentSpec(name="orders", read=_Wide, lenient_read_fields=frozenset({"alpha"}))

    lean_fp = SpecRegistry().register(lean).freeze().fingerprint()
    rich_fp = SpecRegistry().register(rich).freeze().fingerprint()

    assert lean_fp != rich_fp


# ....................... #


def test_spec_fingerprint_addresses_one_entry() -> None:
    registry = SpecRegistry().register(_document("orders"), _document("users")).freeze()

    orders = registry.spec_fingerprint(SpecPlane.DOCUMENT, "orders")

    assert orders != registry.spec_fingerprint(SpecPlane.DOCUMENT, "users")
    assert orders != registry.fingerprint()  # an entry is not the whole inventory


def test_spec_fingerprint_refuses_an_uncatalogued_name() -> None:
    registry = SpecRegistry().register(_document("orders")).freeze()

    with pytest.raises(CoreException, match="not catalogued"):
        registry.spec_fingerprint(SpecPlane.DOCUMENT, "nope")


# ....................... #


def test_a_model_instance_with_a_set_field_hashes_identically_across_processes() -> None:
    """The trap this module exists to avoid, one level in.

    A pydantic *instance* reaching ``_shape`` used to be dumped with ``mode="json"`` — which
    looks like the right answer and is not: pydantic renders a ``set`` field as a **list in
    set-iteration order**, so the dump arrived pre-flattened and already unordered, past the one
    branch that would have sorted it. String hashing is seeded per process, so the same model
    fingerprinted four different ways in four interpreters. Python mode keeps the set a set, and
    the set branch sorts it.
    """

    script = textwrap.dedent(
        """
        from pydantic import BaseModel

        from forze.application.contracts.inventory.fingerprint import _shape
        from forze.base.primitives import stable_payload_fingerprint


        class Policy(BaseModel):
            roles: set[str]


        shaped = _shape(
            Policy(roles={"admin", "billing", "support", "auditor", "owner"}), at="probe"
        )
        print(stable_payload_fingerprint(shaped))
        """
    )

    digests = {
        seed: subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for seed in ("0", "1", "7", "12345")
    }

    assert len(set(digests.values())) == 1, f"model fingerprint varies by hash seed: {digests}"


def test_a_model_instances_scalars_render_rather_than_refuse() -> None:
    """A ``mode="python"`` dump hands back ``UUID`` / ``datetime`` / ``Decimal`` as objects.

    Each has exactly one deterministic textual form, so they are rendered rather than refused —
    the fail-closed branch is for values that have *no* portable form, not for these.
    """

    class Row(BaseModel):
        id: UUID
        at: datetime
        total: Decimal
        tags: set[str]

    row = Row(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        at=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        total=Decimal("10.50"),
        tags={"b", "a"},
    )

    assert _shape(row, at="probe") == {
        "id": "11111111-1111-1111-1111-111111111111",
        "at": "2026-07-15T12:00:00+00:00",
        "total": "10.50",
        "tags": ["a", "b"],
    }


# ....................... #


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(object(), id="bare-object"),
        pytest.param(_document, id="function"),
        pytest.param(b"bytes", id="bytes"),
    ],
)
def test_an_unrenderable_value_is_refused_not_stringified(value: object) -> None:
    """The guard the whole module rests on.

    ``stable_json_bytes`` serializes with ``default=str``, so anything reaching it unrendered is
    hashed as its repr — for a plain object, ``<... object at 0x7f…>``, a memory address that is
    constant in one process and different in the next. Refusing is the only safe answer; a
    fingerprint that silently varies per process is worse than none.
    """

    with pytest.raises(CoreException, match="no portable rendering|not a pydantic model"):
        _shape(value, at="probe")
