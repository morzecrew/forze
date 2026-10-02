"""An ``id`` sort key is ordered by the document name, which forze sets to the id.

Ordered by the stored ``id`` field, a sort on another key plus the ``id`` tie-breaker needs
a composite index; production Firestore refuses the query until one exists (the emulator
does not check). Every index already ends in ``__name__``, so ordering by it needs none.
"""

from unittest.mock import MagicMock

from forze.domain.models import Document
from forze_firestore.kernel.gateways.read import FirestoreReadGateway
from tests.unit._gateway_codec_helpers import codec_for


class _Row(Document):
    grp: int


def _gw() -> FirestoreReadGateway[_Row]:
    return FirestoreReadGateway(
        relation=("db", "t"),
        client=MagicMock(),
        model_type=_Row,
        codec=codec_for(_Row),
        tenant_aware=False,
    )


def test_an_id_key_orders_by_the_document_name() -> None:
    assert _gw().render_sorts({"grp": "desc", "id": "desc"}) == [
        ("grp", "DESCENDING"),
        ("__name__", "DESCENDING"),
    ]


def test_other_keys_keep_their_field() -> None:
    assert _gw().render_sorts({"grp": "asc"}) == [("grp", "ASCENDING")]
