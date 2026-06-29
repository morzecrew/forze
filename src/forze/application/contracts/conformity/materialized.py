"""Shared validation for *materialized* fields (persisted ``@computed_field`` columns).

A materialized field is a ``@computed_field`` that is also a real column on the
relation, so a derived value can be filtered and sorted at the database instead of
being recomputed only between storage and the interface. This module holds the rule
common to every spec that exposes the knob
(:class:`~forze.application.contracts.document.DocumentSpec`,
:class:`~forze.application.contracts.search.SearchSpec`), so they cannot drift.
"""

from pydantic import BaseModel

from forze.base.exceptions import exc

# ----------------------- #


def validate_materialized_computed(
    model: type,
    materialized: frozenset[str],
    *,
    spec_name: object,
    label: str,
) -> None:
    """Validate that *materialized* names are ``@computed_field`` on *model*.

    *model* must be a computed-capable Pydantic model (record models are
    ``BaseModel`` subclasses); a non-Pydantic type is a clean configuration error
    rather than a raw ``AttributeError`` on ``model_computed_fields``.

    :param label: Role of *model* in messages (e.g. ``"read"``, ``"domain"``).
    :raises exc.configuration: when *model* is not computed-capable, or a name is not
        a ``@computed_field`` on it.
    """

    if not materialized:
        return

    if not isinstance(model, type):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise exc.configuration(
            "Materialized fields require a Pydantic model with ``@computed_field``; "
            f"the {label} value {model!r} (spec {spec_name!r}) is not a class.",
        )

    if not issubclass(model, BaseModel):
        raise exc.configuration(
            "Materialized fields require a Pydantic model with ``@computed_field``; "
            f"the {label} model {model.__name__} (spec {spec_name!r}) is not one.",
        )

    if missing := materialized - frozenset(model.model_computed_fields):
        raise exc.configuration(
            f"Materialized field(s) {sorted(missing)} are not ``@computed_field`` on "
            f"the {label} model {model.__name__} (spec {spec_name!r}).",
        )
