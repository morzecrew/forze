from __future__ import annotations

from functools import update_wrapper
from typing import (
    Any,
    Callable,
    Concatenate,
    Generic,
    Optional,
    ParamSpec,
    TypeVar,
    overload,
)

# ----------------------- #

ClsMethod = TypeVar("ClsMethod", bound=Callable[..., Any])
InstMethod = TypeVar("InstMethod", bound=Callable[..., Any])

OwnerT = TypeVar("OwnerT", bound=object)
P = ParamSpec("P")
R = TypeVar("R")

# ....................... #


class hybridmethod(Generic[OwnerT, P, R]):
    """Descriptor that dispatches to a class method or instance method by access context.

    When accessed on the class (e.g. ``Owner.method()``), invokes the class-level
    function. When accessed on an instance, invokes the instance-level function
    registered via :meth:`instancemethod`. The instance method must be registered
    before instance access; otherwise :exc:`AttributeError` is raised.
    """

    __name__: str
    __qualname__: str
    __doc__: Optional[str]
    __module__: str
    _cls_method: Callable[Concatenate[type[OwnerT], P], R]
    _instance_method: Optional[Callable[Concatenate[OwnerT, P], R]]
    _owner: Optional[type[Any]]
    _attr_name: Optional[str]

    # ....................... #

    def __init__(self, cls_method: Callable[Concatenate[type[OwnerT], P], R]) -> None:
        """Initialize the hybrid method with the class-level implementation.

        :param cls_method: Callable invoked when accessed on the class; receives
            the owner type as first argument.
        :raises TypeError: When ``cls_method`` is not callable.
        """

        if not callable(cls_method):
            raise TypeError("hybridmethod requires a callable class-level function")

        self._cls_method = cls_method
        self._instance_method = None
        self._owner = None
        self._attr_name = None

        update_wrapper(self, cls_method)  # type: ignore[arg-type]

    # ....................... #

    def __set_name__(self, owner: type[Any], name: str) -> None:
        """Store the owning class and attribute name for introspection."""

        self._owner = owner
        self._attr_name = name

    # ....................... #

    def instancemethod(
        self,
        method: Callable[Concatenate[OwnerT, P], R],
    ) -> hybridmethod[OwnerT, P, R]:
        """Register the instance-level implementation and return ``self`` for chaining.

        :param method: Callable invoked when accessed on an instance; receives
            the instance as first argument.
        :returns: ``self`` for method chaining.
        """

        self._instance_method = method
        return self

    # ....................... #

    @property
    def cls_method(self) -> Callable[Concatenate[type[OwnerT], P], R]:
        """Class-level callable used when accessed on the owner type."""

        return self._cls_method

    @property
    def inst_method(self) -> Optional[Callable[Concatenate[OwnerT, P], R]]:
        """Instance-level callable, or ``None`` if not registered."""

        return self._instance_method

    # ....................... #

    @overload
    def __get__(
        self,
        obj: None,
        objtype: type[Any] | None = None,
    ) -> Callable[P, R]:
        """Return a bound callable that invokes the class-level method."""
        ...

    @overload
    def __get__(
        self,
        obj: Any,
        objtype: type[Any] | None = None,
    ) -> Callable[P, R]:
        """Return a bound callable that invokes the instance-level method."""
        ...

    def __get__(
        self,
        obj: Any,
        objtype: Optional[type[Any]] = None,
    ) -> Callable[P, R]:
        """Bind the descriptor to a class or instance and return the appropriate callable.

        When ``obj`` is ``None``, binds the class method to ``objtype``. When
        ``obj`` is an instance, binds the instance method to ``obj``. Raises
        :exc:`AttributeError` if accessed on an instance without a registered
        instance method.

        :raises TypeError: When ``obj`` is ``None`` and ``objtype`` is ``None``.
        :raises AttributeError: When accessed on an instance and no instance
            method was registered via :meth:`instancemethod`.
        """

        if obj is None:
            if objtype is None:
                raise TypeError("objtype is required for class access")

            def bound(*args: P.args, **kwargs: P.kwargs) -> R:
                return self._cls_method(objtype, *args, **kwargs)

            update_wrapper(bound, self._cls_method)
            return bound

        if self._instance_method is None:
            name = getattr(self.cls_method, "__name__", "<unknown>")
            owner_name = type(obj).__name__
            raise AttributeError(
                f"'{owner_name}.{name}' is not available on instances: "
                "no instance-level implementation was registered"
            )

        else:

            def bound(*args: P.args, **kwargs: P.kwargs) -> R:
                return self._instance_method(obj, *args, **kwargs)  # type: ignore[misc]

            update_wrapper(bound, self._instance_method)
            return bound

    # ....................... #

    def _bind_class(self, owner: type[Any]) -> Callable[..., Any]:
        """Bind the class method to the given owner type."""

        func = self._cls_method

        def bound(*args: Any, **kwargs: Any) -> Any:
            return func(owner, *args, **kwargs)

        update_wrapper(bound, func)
        return bound

    # ....................... #

    def _bind_instance(self, obj: Any) -> Callable[..., Any]:
        """Bind the instance method to the given instance."""

        func = self._instance_method
        if func is None:
            raise RuntimeError("Instance function is unexpectedly missing")

        def bound(*args: Any, **kwargs: Any) -> Any:
            return func(obj, *args, **kwargs)

        update_wrapper(bound, func)
        return bound

    # ....................... #

    def __repr__(self) -> str:
        """Return a string representation for debugging."""

        name = self._attr_name or getattr(self._cls_method, "__name__", "<unnamed>")
        owner = self._owner.__name__ if self._owner is not None else "?"
        has_instance = self._instance_method is not None
        return (
            f"<hybridmethod {owner}.{name} "
            f"class_func={self._cls_method!r} instance_registered={has_instance}>"
        )
