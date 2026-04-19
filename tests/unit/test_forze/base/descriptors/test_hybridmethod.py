"""Unit tests for forze.base.descriptors.hybridmethod."""

import pytest

from forze.base.descriptors import hybridmethod

# ----------------------- #


class SampleClass:
    """Sample class using hybridmethod for testing."""

    @hybridmethod
    def merge(cls, *items: "SampleClass") -> "SampleClass":
        """Merge items (class-level)."""
        acc = SampleClass(value=0)
        for item in items:
            acc = SampleClass(value=acc.value + item.value)
        return acc

    @merge.instancemethod
    def _merge_instance(self, *items: "SampleClass") -> "SampleClass":
        """Merge items (instance-level)."""
        return type(self).merge(self, *items)

    def __init__(self, value: int = 0) -> None:
        self.value = value


class SampleWithoutInstance:
    """Sample class with hybridmethod but no instance implementation."""

    @hybridmethod
    def class_only(cls) -> str:
        """Class-only method."""
        return f"class:{cls.__name__}"


# ----------------------- #


class TestHybridmethod:
    """Tests for hybridmethod descriptor."""

    def test_class_call_invokes_cls_method(self) -> None:
        """Class-level call passes class as first argument."""
        a = SampleClass(value=1)
        b = SampleClass(value=2)
        merged = SampleClass.merge(a, b)
        assert merged.value == 3

    def test_instance_call_invokes_instance_method(self) -> None:
        """Instance-level call merges self with other items."""
        a = SampleClass(value=1)
        b = SampleClass(value=2)
        c = SampleClass(value=3)
        merged = a.merge(b, c)
        assert merged.value == 6  # 1 + 2 + 3

    def test_instance_call_without_instance_method_raises(self) -> None:
        """Accessing hybridmethod on instance without instancemethod raises."""
        obj = SampleWithoutInstance()
        with pytest.raises(
            AttributeError,
            match="not available on instances.*no instance-level implementation",
        ):
            obj.class_only()

    def test_class_call_without_objtype_raises(self) -> None:
        """Class access without objtype raises (edge case via __get__)."""
        hm = SampleClass.merge
        # When obj is None and objtype is None, __get__ raises
        # This is hard to trigger via normal attribute access; we test the descriptor directly
        from forze.base.descriptors.hybridmethod import hybridmethod

        class Bare:
            @hybridmethod
            def m(cls):
                return cls

        desc = Bare.__dict__["m"]
        with pytest.raises(TypeError, match="objtype is required"):
            desc.__get__(None, None)

    def test_instancemethod_decorator_registers_method(self) -> None:
        """instancemethod decorator correctly registers the instance implementation."""
        merge_desc = SampleClass.__dict__["merge"]
        assert merge_desc.inst_method is not None
        class_only_desc = SampleWithoutInstance.__dict__["class_only"]
        assert class_only_desc.inst_method is None

    def test_cls_method_property(self) -> None:
        """cls_method property returns the class-level callable."""
        merge_desc = SampleClass.__dict__["merge"]
        cls_m = merge_desc.cls_method
        assert callable(cls_m)
        a = SampleClass(value=1)
        b = SampleClass(value=2)
        merged = cls_m(SampleClass, a, b)
        assert merged.value == 3

    def test_repr(self) -> None:
        """__repr__ includes owner, name, and instance_registered."""
        merge_desc = SampleClass.__dict__["merge"]
        r = repr(merge_desc)
        assert "hybridmethod" in r
        assert "SampleClass" in r
        assert "merge" in r
        assert "instance_registered=True" in r

        class_only_desc = SampleWithoutInstance.__dict__["class_only"]
        r2 = repr(class_only_desc)
        assert "instance_registered=False" in r2

    def test_init_requires_callable(self) -> None:
        """hybridmethod requires a callable for cls_method."""
        with pytest.raises(TypeError, match="requires a callable"):
            hybridmethod(123)  # type: ignore[arg-type]

    def test_set_name_sets_owner_and_attr(self) -> None:
        """__set_name__ stores owner and attribute name."""
        merge_desc = SampleClass.__dict__["merge"]
        assert merge_desc._owner is SampleClass
        # _attr_name can be "merge" or "_merge_instance" depending on class dict order,
        # since the same descriptor is stored under both names when using @merge.instancemethod
        assert merge_desc._attr_name in ("merge", "_merge_instance")

    def test_bind_class_invokes_cls_method(self) -> None:
        """_bind_class returns a callable that forwards to the class-level function."""
        desc = SampleClass.__dict__["merge"]
        bound = desc._bind_class(SampleClass)
        a = SampleClass(value=1)
        b = SampleClass(value=2)
        merged = bound(a, b)
        assert merged.value == 3

    def test_bind_instance_invokes_instance_method(self) -> None:
        """_bind_instance returns a callable that forwards to the instance-level function."""
        desc = SampleClass.__dict__["merge"]
        obj = SampleClass(value=10)
        bound = desc._bind_instance(obj)
        merged = bound(SampleClass(value=1))
        assert merged.value == 11

    def test_bind_instance_raises_when_not_registered(self) -> None:
        """_bind_instance raises when no instance implementation exists."""
        desc = SampleWithoutInstance.__dict__["class_only"]
        with pytest.raises(RuntimeError, match="unexpectedly missing"):
            desc._bind_instance(SampleWithoutInstance())
