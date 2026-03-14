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
        if not callable(cls_method):
            raise TypeError("hybridmethod requires a callable class-level function")

        self._cls_method = cls_method
        self._instance_method = None
        self._owner = None
        self._attr_name = None

        update_wrapper(self, cls_method)  # type: ignore[arg-type]

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._owner = owner
        self._attr_name = name

    def instancemethod(
        self,
        method: Callable[Concatenate[OwnerT, P], R],
    ) -> hybridmethod[OwnerT, P, R]:
        self._instance_method = method
        return self

    @property
    def cls_method(self) -> Callable[Concatenate[type[OwnerT], P], R]:
        return self._cls_method

    @property
    def inst_method(self) -> Optional[Callable[Concatenate[OwnerT, P], R]]:
        return self._instance_method

    @overload
    def __get__(
        self,
        obj: None,
        objtype: type[Any] | None = None,
    ) -> Callable[P, R]: ...

    @overload
    def __get__(
        self,
        obj: Any,
        objtype: type[Any] | None = None,
    ) -> Callable[P, R]: ...

    def __get__(
        self,
        obj: Any,
        objtype: Optional[type[Any]] = None,
    ) -> Callable[P, R]:
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

    def _bind_class(self, owner: type[Any]) -> Callable[..., Any]:
        func = self._cls_method

        def bound(*args: Any, **kwargs: Any) -> Any:
            return func(owner, *args, **kwargs)

        update_wrapper(bound, func)
        return bound

    def _bind_instance(self, obj: Any) -> Callable[..., Any]:
        func = self._instance_method
        if func is None:
            raise RuntimeError("Instance function is unexpectedly missing")

        def bound(*args: Any, **kwargs: Any) -> Any:
            return func(obj, *args, **kwargs)

        update_wrapper(bound, func)
        return bound

    def __repr__(self) -> str:
        name = self._attr_name or getattr(self._cls_method, "__name__", "<unnamed>")
        owner = self._owner.__name__ if self._owner is not None else "?"
        has_instance = self._instance_method is not None
        return (
            f"<hybridmethod {owner}.{name} "
            f"class_func={self._cls_method!r} instance_registered={has_instance}>"
        )
