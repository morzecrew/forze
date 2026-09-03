"""The reflection gates catch what they claim to and refuse to pass vacuously."""

from __future__ import annotations

import importlib
import sys
import textwrap
import types
from pathlib import Path
from typing import Annotated, Protocol
from uuid import UUID

import pytest

from forze.testing import (
    assert_operation_namespaces,
    assert_pure_module,
    assert_scope_first,
)

pytestmark = pytest.mark.unit

# ----------------------- #
# Purity gate


def _module_from_source(tmp_path: Path, name: str, source: str) -> types.ModuleType:
    path = tmp_path / f"{name}.py"
    path.write_text(textwrap.dedent(source))

    module = types.ModuleType(name)
    module.__file__ = str(path)

    return module


class TestPurityGate:
    def test_allowed_imports_pass(self, tmp_path: Path) -> None:
        module = _module_from_source(
            tmp_path,
            "engine",
            """
            import math
            from decimal import Decimal
            from . import sibling
            """,
        )

        assert_pure_module(module, allowed=["math", "decimal"])

    def test_unlisted_import_fails_with_location(self, tmp_path: Path) -> None:
        module = _module_from_source(tmp_path, "engine", "import math\nimport socket\n")

        with pytest.raises(AssertionError, match=r"engine\.py:2: import of unlisted.*'socket'"):
            assert_pure_module(module, allowed=["math"])

    def test_forbidden_import_fails(self, tmp_path: Path) -> None:
        module = _module_from_source(tmp_path, "engine", "from time import sleep\n")

        with pytest.raises(AssertionError, match="forbidden module 'time'"):
            assert_pure_module(module, allowed=["math"], forbidden=["time"])

    def test_contradictory_lists_are_refused(self, tmp_path: Path) -> None:
        module = _module_from_source(tmp_path, "engine", "import math\n")

        with pytest.raises(AssertionError, match="allows and forbids the same roots"):
            assert_pure_module(module, allowed=["time"], forbidden=["time"])

    def test_own_package_and_future_are_implicitly_allowed(self, tmp_path: Path) -> None:
        pkg = tmp_path / "engine"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("from __future__ import annotations\n")
        (pkg / "core.py").write_text("import math\nfrom engine import helpers\n")
        (pkg / "helpers.py").write_text("")

        module = types.ModuleType("engine")
        module.__path__ = [str(pkg)]  # type: ignore[attr-defined]

        assert_pure_module(module, allowed=["math"])

    def test_empty_package_is_refused(self, tmp_path: Path) -> None:
        module = types.ModuleType("hollow")
        module.__path__ = [str(tmp_path / "nothing")]  # type: ignore[attr-defined]

        with pytest.raises(AssertionError, match="discovered no source files"):
            assert_pure_module(module, allowed=["math"])

    def test_unparsable_file_is_a_gate_failure(self, tmp_path: Path) -> None:
        # The contract is an AssertionError listing everything wrong, never a raw
        # SyntaxError escaping mid-walk.
        module = _module_from_source(tmp_path, "engine", "def broken(:\n")

        with pytest.raises(AssertionError, match="could not parse"):
            assert_pure_module(module, allowed=["math"])

    def test_pep263_encoding_cookie_is_honored(self, tmp_path: Path) -> None:
        # A valid non-UTF-8 source must be parsed, not reported as a false violation.
        path = tmp_path / "legacy.py"
        path.write_bytes("# -*- coding: latin-1 -*-\n# caf\xe9\nimport math\n".encode("latin-1"))

        module = types.ModuleType("legacy")
        module.__file__ = str(path)

        assert_pure_module(module, allowed=["math"])

    def test_sourceless_module_is_refused(self) -> None:
        with pytest.raises(AssertionError, match="no source file"):
            assert_pure_module(types.ModuleType("synthetic"), allowed=["math"])

    def test_all_violations_reported_at_once(self, tmp_path: Path) -> None:
        module = _module_from_source(tmp_path, "engine", "import socket\nimport json\n")

        with pytest.raises(AssertionError) as ei:
            assert_pure_module(module, allowed=[])

        assert "'socket'" in str(ei.value) and "'json'" in str(ei.value)


# ----------------------- #
# Scope-first gate


def _ports_module(name: str, **classes: type) -> types.ModuleType:
    module = types.ModuleType(name)

    for cls_name, cls in classes.items():
        cls.__module__ = name
        setattr(module, cls_name, cls)

    sys.modules[name] = module

    return module


def _good_port() -> type:
    class GoodPort(Protocol):
        async def read(self, tenant_id: UUID, /, key: str) -> str: ...
        async def write(self, tenant_id: UUID, /, key: str, value: str) -> None: ...

    return GoodPort


def _keyword_port() -> type:
    class KeywordPort(Protocol):
        async def read(self, tenant_id: UUID, key: str) -> str: ...

    return KeywordPort


class TestScopeFirstGate:
    def test_compliant_ports_pass(self) -> None:
        module = _ports_module("gate_ports_good", GoodPort=_good_port())

        assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_static_and_class_methods_are_checked(self) -> None:
        # getattr_static hands back descriptors; a silently skipped member would be a
        # hole in the gate. A static method has no receiver to skip.
        class Port(Protocol):
            @staticmethod
            async def read(key: str) -> str: ...

            @classmethod
            async def scan(cls, owner: UUID, /) -> str: ...

        module = _ports_module("gate_ports_desc", Port=Port)

        with pytest.raises(AssertionError) as ei:
            assert_scope_first(module, name="tenant_id", annotation=UUID)

        message = str(ei.value)
        assert "Port.read: first parameter is 'key'" in message
        assert "Port.scan: first parameter is 'owner'" in message

    def test_receiver_is_skipped_by_position_not_spelling(self) -> None:
        class Port(Protocol):
            async def read(this, tenant_id: UUID, /) -> str: ...

        module = _ports_module("gate_ports_recv", Port=Port)

        assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_unresolvable_annotation_is_a_violation_not_a_crash(self, tmp_path: Path) -> None:
        # A TYPE_CHECKING-only import leaves the annotation unresolvable at runtime;
        # the gate must report it and keep going, never crash with a NameError.
        path = tmp_path / "lazy_ports.py"
        path.write_text(
            textwrap.dedent(
                """
                from __future__ import annotations
                from typing import TYPE_CHECKING, Protocol
                if TYPE_CHECKING:
                    from uuid import UUID
                class Port(Protocol):
                    async def read(self, tenant_id: UUID, /) -> str: ...
                """
            )
        )
        sys.path.insert(0, str(tmp_path))

        try:
            lazy_ports = importlib.import_module("lazy_ports")
        finally:
            sys.path.remove(str(tmp_path))

        with pytest.raises(AssertionError, match="unresolvable"):
            assert_scope_first(lazy_ports, name="tenant_id", annotation=UUID)

    def test_annotated_metadata_is_preserved(self) -> None:
        class Port(Protocol):
            async def read(self, tenant_id: Annotated[UUID, "owner"], /) -> str: ...

        module = _ports_module("gate_ports_annotated", Port=Port)

        assert_scope_first(module, name="tenant_id", annotation=Annotated[UUID, "owner"])

    def test_broken_sibling_annotation_is_not_blamed_on_the_key(self, tmp_path: Path) -> None:
        # get_type_hints over the whole function would fail on the return annotation
        # and misattribute it; only the key's own annotation is resolved.
        path = tmp_path / "sibling_ports.py"
        path.write_text(
            textwrap.dedent(
                """
                from __future__ import annotations
                from typing import Protocol
                from uuid import UUID
                class Port(Protocol):
                    async def read(self, tenant_id: UUID, /) -> Broken: ...
                """
            )
        )
        sys.path.insert(0, str(tmp_path))

        try:
            sibling_ports = importlib.import_module("sibling_ports")
        finally:
            sys.path.remove(str(tmp_path))

        assert_scope_first(sibling_ports, name="tenant_id", annotation=UUID)

    def test_invalid_annotation_syntax_is_a_violation_not_a_crash(self) -> None:
        class Port(Protocol):
            async def read(self, tenant_id: "not valid !", /) -> str: ...  # type: ignore[valid-type]  # noqa: F722

        module = _ports_module("gate_ports_synerr", Port=Port)

        with pytest.raises(AssertionError, match="unresolvable"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_unannotated_key_is_a_violation(self, tmp_path: Path) -> None:
        # Defined in its own module without deferred annotations, so the missing
        # annotation is genuinely absent rather than an empty string.
        path = tmp_path / "bare_ports.py"
        path.write_text(
            textwrap.dedent(
                """
                from typing import Protocol
                class Port(Protocol):
                    async def read(self, tenant_id, /) -> str: ...
                """
            )
        )
        sys.path.insert(0, str(tmp_path))

        try:
            bare_ports = importlib.import_module("bare_ports")
        finally:
            sys.path.remove(str(tmp_path))

        with pytest.raises(AssertionError, match="carries no annotation"):
            assert_scope_first(bare_ports, name="tenant_id", annotation=UUID)

    def test_live_none_annotation_is_normalized(self, tmp_path: Path) -> None:
        # get_type_hints normalizes a live None annotation to NoneType; the gate must
        # apply the same normalization to non-string annotations before comparing.
        path = tmp_path / "none_ports.py"
        path.write_text(
            textwrap.dedent(
                """
                from typing import Protocol
                class Port(Protocol):
                    async def read(self, marker: None, /) -> str: ...
                """
            )
        )
        sys.path.insert(0, str(tmp_path))

        try:
            none_ports = importlib.import_module("none_ports")
        finally:
            sys.path.remove(str(tmp_path))

        assert_scope_first(none_ports, name="marker", annotation=type(None))

    def test_live_object_annotation_is_compared_directly(self, tmp_path: Path) -> None:
        # Without deferred annotations the hint is already an object, not a string.
        path = tmp_path / "live_ports.py"
        path.write_text(
            textwrap.dedent(
                """
                from typing import Protocol
                from uuid import UUID
                class Port(Protocol):
                    async def read(self, tenant_id: UUID, /) -> str: ...
                """
            )
        )
        sys.path.insert(0, str(tmp_path))

        try:
            live_ports = importlib.import_module("live_ports")
        finally:
            sys.path.remove(str(tmp_path))

        assert_scope_first(live_ports, name="tenant_id", annotation=UUID)

    def test_non_method_members_are_skipped(self) -> None:
        class Port(Protocol):
            marker: str

            async def read(self, tenant_id: UUID, /) -> str: ...

        module = _ports_module("gate_ports_attr", Port=Port)

        assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_keyword_capable_parameter_fails(self) -> None:
        # The whole mechanism: a keyword parameter can be omitted; positional-only cannot.
        module = _ports_module("gate_ports_kw", KeywordPort=_keyword_port())

        with pytest.raises(AssertionError, match="not positional-only"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_default_fails(self) -> None:
        class Port(Protocol):
            async def read(self, tenant_id: UUID | None = None, /) -> str: ...

        module = _ports_module("gate_ports_def", Port=Port)

        with pytest.raises(AssertionError, match="carries a default"):
            assert_scope_first(module, name="tenant_id", annotation=UUID | None)

    def test_wrong_name_fails(self) -> None:
        class Port(Protocol):
            async def read(self, owner: UUID, /) -> str: ...

        module = _ports_module("gate_ports_name", Port=Port)

        with pytest.raises(AssertionError, match="first parameter is 'owner'"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_wrong_annotation_fails(self) -> None:
        class Port(Protocol):
            async def read(self, tenant_id: str, /) -> str: ...

        module = _ports_module("gate_ports_type", Port=Port)

        with pytest.raises(AssertionError, match="annotated"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_parameterless_method_fails(self) -> None:
        class Port(Protocol):
            async def ping(self) -> None: ...

        module = _ports_module("gate_ports_none", Port=Port)

        with pytest.raises(AssertionError, match="takes no parameter"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_exclusion_skips_a_named_method(self) -> None:
        module = _ports_module(
            "gate_ports_excl", GoodPort=_good_port(), KeywordPort=_keyword_port()
        )

        assert_scope_first(module, name="tenant_id", annotation=UUID, exclude=["KeywordPort.read"])

    def test_stale_exclusion_fails(self) -> None:
        # An exclusion matching nothing is a rename that silently widened the gate.
        module = _ports_module("gate_ports_stale", GoodPort=_good_port())

        with pytest.raises(AssertionError, match="exclusion matches no method"):
            assert_scope_first(module, name="tenant_id", annotation=UUID, exclude=["Gone.read"])

    def test_module_without_protocols_is_refused(self) -> None:
        module = types.ModuleType("gate_ports_empty")

        with pytest.raises(AssertionError, match="no Protocols"):
            assert_scope_first(module, name="tenant_id", annotation=UUID)

    def test_fully_excluded_module_is_refused(self) -> None:
        module = _ports_module("gate_ports_allexcl", KeywordPort=_keyword_port())

        with pytest.raises(AssertionError, match="checked no methods"):
            assert_scope_first(
                module, name="tenant_id", annotation=UUID, exclude=["KeywordPort.read"]
            )


# ----------------------- #
# Operation-namespace gate


class TestOperationNamespaceGate:
    def test_disjoint_namespaced_edges_pass(self) -> None:
        assert_operation_namespaces(
            {
                "product": ["product.orders.list", "product.orders.get"],
                "operator": ["operator.orders.list"],
            }
        )

    def test_foreign_prefix_fails(self) -> None:
        with pytest.raises(AssertionError, match=r"'operator\.orders\.list' is not under"):
            assert_operation_namespaces(
                {
                    "product": ["product.orders.list", "operator.orders.list"],
                    "operator": ["operator.x"],
                }
            )

    def test_bare_edge_name_is_not_namespaced(self) -> None:
        with pytest.raises(AssertionError, match="'product' is not under"):
            assert_operation_namespaces({"product": ["product"]})

    def test_shared_id_across_edges_fails(self) -> None:
        # An id under one prefix registered on both edges: disjointness is its own check.
        with pytest.raises(AssertionError, match="appears on both"):
            assert_operation_namespaces(
                {
                    "a": ["a.op"],
                    "a.b": ["a.b.op", "a.op"],
                }
            )

    def test_empty_mapping_is_refused(self) -> None:
        with pytest.raises(AssertionError, match="no edges"):
            assert_operation_namespaces({})

    def test_empty_edge_is_refused(self) -> None:
        with pytest.raises(AssertionError, match=r"'ghost'.*no operation ids"):
            assert_operation_namespaces({"product": ["product.x"], "ghost": []})
