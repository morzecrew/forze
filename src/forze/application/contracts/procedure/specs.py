"""Declarative specification for a governed parametrized command/compute operation."""

from typing import Any, final

import attrs
from pydantic import BaseModel, TypeAdapter, ValidationError

from forze.base.exceptions import exc
from forze.base.serialization import (
    ModelCodec,
    default_model_codec,
    stored_field_names_for,
)

from ..base import BaseSpec
from ..crypto import FieldEncryption

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProcedureSpec[In: BaseModel, Out](BaseSpec):
    """Specification for one governed parametrized DB command/compute operation.

    **One spec = one procedure** (unlike :class:`~forze.application.contracts.analytics.AnalyticsSpec`,
    which groups a map of named queries). A procedure is a heterogeneous unit — a set-based
    recompute, a ``CALL``, a materialized-view ``REFRESH``, a compute function invoked for effect
    — so there is no shared relation family to group under one name; grouping would be arbitrary
    indirection. The input is a typed model and the output an optional typed result; the backend
    adapter dispatches on :attr:`result` cardinality (``None`` -> affected count, a scalar type ->
    single value, a Pydantic model -> single row).

    The port is **command-only**: it cannot be acquired in a read-only (``QUERY``) operation. See
    :class:`~forze.application.contracts.procedure.ports.ProcedurePort`.
    """

    params: type[In]
    """Pydantic model for the bound parameters passed to ``run``."""

    result: type[Out] | None = attrs.field(default=None)
    """Output type: a Pydantic model (single row), a scalar type (single value), or ``None`` for
    a side-effect-only procedure (returns an affected-row count)."""

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy applied to **params**. ``binds_record_id`` is unsupported — procedure params have no stable record id to bind into the AAD. ``None`` (default) = no encryption."""

    params_codec: ModelCodec[In, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Param codec; defaults to :func:`default_model_codec` for :attr:`params`. The adapter
    factory rebuilds this to apply :attr:`encryption` when a keyring is wired."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description for documentation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_procedure_spec(self)

    # ....................... #

    @property
    def resolved_params_codec(self) -> ModelCodec[In, Any]:
        """Param codec (explicit override or :func:`default_model_codec`)."""

        if self.params_codec is not None:
            return self.params_codec

        return default_model_codec(self.params)

    # ....................... #

    @property
    def returns_row(self) -> bool:
        """Whether :attr:`result` is a single typed row (a Pydantic model)."""

        return (
            self.result is not None
            and isinstance(self.result, type)  # pyright: ignore[reportUnnecessaryIsInstance]
            and issubclass(self.result, BaseModel)
        )

    # ....................... #

    @property
    def returns_scalar(self) -> bool:
        """Whether :attr:`result` is a single scalar value (a non-model type)."""

        return self.result is not None and not self.returns_row

    # ....................... #

    def coerce_scalar(self, value: Any) -> Any:
        """Validate/coerce a scalar fetch result to the declared scalar :attr:`result` type.

        Mirrors how a row result is validated through its codec: a non-``None`` value is coerced
        via Pydantic (lax), so a value of the wrong type fails at the procedure boundary rather
        than surfacing as a different runtime type. ``None`` (SQL ``NULL``) passes through, and a
        ``None`` :attr:`result` is a no-op.
        """

        if value is None or self.result is None:
            return value

        try:
            return TypeAdapter(self.result).validate_python(value)

        except ValidationError as e:
            raise exc.validation(
                f"Procedure {self.name!r} scalar result must be "
                f"{self.result.__name__}, got {type(value).__name__}."
            ) from e


# ....................... #


def validate_procedure_spec(spec: ProcedureSpec[Any, Any]) -> None:
    """Check internal consistency; raise on violation.

    :param spec: Procedure specification to validate.
    """

    if not (
        isinstance(spec.params, type)  # pyright: ignore[reportUnnecessaryIsInstance]
        and issubclass(spec.params, BaseModel)
    ):
        raise exc.configuration("ProcedureSpec.params must be a Pydantic BaseModel subclass.")

    if spec.result is not None and not isinstance(spec.result, type):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise exc.configuration(
            "ProcedureSpec.result must be a type — a Pydantic model (single row) or a scalar "
            "type (single value) — or None for a side-effect-only procedure."
        )

    if spec.encryption is not None:
        if spec.encryption.binds_record_id:
            raise exc.configuration(
                "ProcedureSpec.encryption cannot set binds_record_id: procedure params have no "
                "stable record id to bind into the AAD. Use a FieldEncryption without it."
            )

        spec.encryption.validate_fields_exist(
            stored_field_names_for(spec.params), spec_name=spec.name
        )
