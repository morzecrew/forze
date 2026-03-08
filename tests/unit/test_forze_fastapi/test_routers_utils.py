"""Unit tests for forze_fastapi.routers._utils."""

from forze_fastapi.routers._utils import extend_doc, override_annotations, override_doc


# ----------------------- #


class TestOverrideDoc:
    """Tests for override_doc decorator."""

    def test_sets_docstring(self) -> None:
        """override_doc sets the docstring of the decorated object."""

        @override_doc("New docstring")
        def fn() -> None:
            """Original."""
            pass

        assert fn.__doc__ == "New docstring"

    def test_overrides_existing(self) -> None:
        """override_doc replaces existing docstring."""

        def fn() -> None:
            """Original."""
            pass

        decorated = override_doc("Replaced")(fn)
        assert decorated.__doc__ == "Replaced"


class TestExtendDoc:
    """Tests for extend_doc decorator."""

    def test_appends_to_existing(self) -> None:
        """extend_doc appends to existing docstring with default sep."""

        @extend_doc("Additional content")
        def fn() -> None:
            """Base."""
            pass

        assert "Base" in (fn.__doc__ or "")
        assert "Additional content" in (fn.__doc__ or "")

    def test_custom_sep(self) -> None:
        """extend_doc uses custom separator."""

        @extend_doc("Extra", sep=" | ")
        def fn() -> None:
            """Base"""
            pass

        assert fn.__doc__ == "Base | Extra"

    def test_empty_base_uses_extra_only(self) -> None:
        """extend_doc with no base uses extra as docstring."""

        def fn() -> None:
            pass

        decorated = extend_doc("Only this")(fn)
        assert decorated.__doc__ == "Only this"


class TestOverrideAnnotations:
    """Tests for override_annotations decorator."""

    def test_sets_annotations(self) -> None:
        """override_annotations sets annotations on the decorated object."""

        @override_annotations({"x": int, "y": str})
        def fn(x, y) -> None:
            pass

        assert fn.__annotations__["x"] is int
        assert fn.__annotations__["y"] is str

    def test_overrides_existing_annotations(self) -> None:
        """override_annotations replaces existing annotations."""

        def fn(x: float, y: str) -> None:
            pass

        decorated = override_annotations({"x": int})(fn)
        assert decorated.__annotations__["x"] is int
        assert decorated.__annotations__["y"] is str
