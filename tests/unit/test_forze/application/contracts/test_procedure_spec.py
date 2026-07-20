"""Unit tests for the procedures contract spec and deps."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.procedure import (
    ExecResult,
    ProcedureCommandDepKey,
    ProcedureDeps,
    ProcedureSpec,
    validate_procedure_spec,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _Params(BaseModel):
    window: str = "2026-01-01"
    secret: str = "x"


class _Row(BaseModel):
    total: int = 0


# ....................... #


class TestProcedureSpec:
    def test_side_effect_spec(self) -> None:
        spec = ProcedureSpec(name="recompute", params=_Params)
        assert spec.result is None
        assert not spec.returns_row
        assert not spec.returns_scalar

    def test_row_spec(self) -> None:
        spec = ProcedureSpec(name="compute_row", params=_Params, result=_Row)
        assert spec.returns_row
        assert not spec.returns_scalar

    def test_scalar_spec(self) -> None:
        spec = ProcedureSpec(name="compute_value", params=_Params, result=int)
        assert spec.returns_scalar
        assert not spec.returns_row

    def test_resolved_params_codec_defaults(self) -> None:
        spec = ProcedureSpec(name="p", params=_Params)
        assert spec.resolved_params_codec.model_type is _Params

    def test_invalid_params_type_raises(self) -> None:
        with pytest.raises(CoreException, match="BaseModel"):
            ProcedureSpec(name="p", params=str)  # type: ignore[arg-type]

    def test_invalid_result_type_raises(self) -> None:
        with pytest.raises(CoreException, match="must be a type"):
            ProcedureSpec(name="p", params=_Params, result=object())  # type: ignore[arg-type]

    def test_encryption_params_first(self) -> None:
        spec = ProcedureSpec(
            name="p",
            params=_Params,
            encryption=FieldEncryption(encrypted=frozenset({"secret"})),
        )
        assert spec.encryption is not None

    def test_encryption_binds_record_id_rejected(self) -> None:
        with pytest.raises(CoreException, match="binds_record_id"):
            ProcedureSpec(
                name="p",
                params=_Params,
                encryption=FieldEncryption(
                    encrypted=frozenset({"secret"}), binds_record_id=True
                ),
            )

    def test_encryption_unknown_field_rejected(self) -> None:
        with pytest.raises(CoreException):
            ProcedureSpec(
                name="p",
                params=_Params,
                encryption=FieldEncryption(encrypted=frozenset({"nope"})),
            )


class TestValidateProcedureSpec:
    def test_validate_accepts_minimal(self) -> None:
        validate_procedure_spec(ProcedureSpec(name="p", params=_Params))


class TestProcedureDeps:
    def test_dep_key_name(self) -> None:
        assert ProcedureCommandDepKey.name == "procedure_command"

    def test_command_only_surface(self) -> None:
        # Procedures is command-only: no query accessor exists.
        assert hasattr(ProcedureDeps, "command")
        assert not hasattr(ProcedureDeps, "query")


class TestExecResult:
    def test_scalar_value(self) -> None:
        assert ExecResult(value=42).value == 42

    def test_affected_count(self) -> None:
        assert ExecResult(affected_count=7).affected_count == 7
