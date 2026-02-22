import hashlib
from typing import Any, TypedDict

import orjson
from pydantic import BaseModel

# ----------------------- #


def pydantic_validate[M: BaseModel](cls: type[M], data: dict[str, Any]) -> M:
    return cls.model_validate(data)


# ....................... #


class _PydanticDumpExcludeOptions(TypedDict, total=False):
    unset: bool
    none: bool
    defaults: bool
    computed_fields: bool


def pydantic_dump(
    obj: BaseModel,
    *,
    exclude: _PydanticDumpExcludeOptions = {},
) -> dict[str, Any]:
    return obj.model_dump(
        exclude_unset=exclude.get("unset", False),
        exclude_none=exclude.get("none", False),
        exclude_defaults=exclude.get("defaults", False),
        exclude_computed_fields=exclude.get("computed_fields", False),
        mode="json",
    )


# ....................... #


def pydantic_field_names(
    cls: type[BaseModel],
    *,
    include_computed: bool = True,
) -> set[str]:
    model_fields = set(cls.model_fields.keys())

    if include_computed:
        model_fields |= set(cls.model_computed_fields.keys())

    return model_fields


# ....................... #


def pydantic_model_hash(
    model: BaseModel,
    *,
    exclude: _PydanticDumpExcludeOptions = {},
) -> str:
    data = pydantic_dump(model, exclude=exclude)
    raw = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)

    return hashlib.sha256(raw).hexdigest()


# # ....................... #
# #! TODO: move somewhere else ...


# def _unwrap_annotated(tp: Any) -> Any:
#     """Annotated[T, ...] -> T (only type part)."""
#     if get_origin(tp) is Annotated:
#         return get_args(tp)[0]

#     return tp


# def _unwrap_optional(tp: Any) -> tuple[bool, Any]:
#     origin = get_origin(tp)

#     if origin is Union:
#         args = list(get_args(tp))

#         if type(None) in args:
#             args.remove(type(None))
#             inner = args[0] if len(args) == 1 else Union[tuple(args)]
#             return True, inner

#     return False, tp


# def _unwrap_type(tp: Any) -> Any:
#     nullable, inner = _unwrap_optional(tp)
#     inner = _unwrap_annotated(inner)
#     inner = _unwrap_annotated(inner)

#     return nullable, inner


# def _is_pydantic_model(tp: Any) -> bool:
#     return isinstance(tp, type) and issubclass(tp, BaseModel)


# def _base_type(tp: Any) -> str:
#     _, tp = _unwrap_type(tp)
#     origin = get_origin(tp)

#     if origin in (list, tuple, set):
#         return "array"

#     if origin is dict:
#         return "object"

#     if tp is str:
#         return "string"

#     if tp in (int, float, Decimal):
#         return "number"

#     if tp is bool:
#         return "boolean"

#     if tp is datetime:
#         return "datetime"

#     if tp is date:
#         return "date"

#     if tp is UUID:
#         return "uuid"

#     if isinstance(tp, type) and issubclass(tp, Enum):
#         if issubclass(tp, StrEnum):
#             return "string"

#         vals = [m.value for m in tp]

#         if vals and isinstance(vals[0], (int, float, Decimal)):
#             return "number"

#         return "string"

#     return "object"


# # def _enum_values(tp: Any) -> Optional[list[Any]]:
# #     _, tp = _unwrap_type(tp)

# #     if isinstance(tp, type) and issubclass(tp, Enum):
# #         return [m.value for m in tp]

# #     origin = get_origin(tp)

# #     if origin is not None and str(origin) == "typing.Literal":
# #         return list(get_args(tp))

# #     return None


# def _pascal_to_snake(name: str) -> str:
#     return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# def _iter_annotated_meta(tp: Any) -> list[Any]:
#     origin = get_origin(tp)

#     if origin is Annotated:
#         return list(get_args(tp)[1:])

#     if origin is Union:
#         metas: list[Any] = []

#         for a in get_args(tp):
#             if a is not type(None):
#                 metas.extend(_iter_annotated_meta(a))

#         return metas

#     return []


# def _iter_all_meta(field: FieldInfo) -> list[Any]:
#     metas: list[Any] = []
#     metas.extend(_iter_annotated_meta(field.metadata))
#     metas.extend(_iter_annotated_meta(field.annotation))

#     return metas


# def _constraints(field: FieldInfo) -> dict[str, Any]:
#     out: dict[str, Any] = {}

#     for m in _iter_all_meta(field):
#         name = m.__class__.__name__

#         if name == "StringConstraints":
#             for k in ["min_length", "max_length", "pattern"]:
#                 v = getattr(m, k, None)

#                 if v is not None:
#                     out[k] = v

#         if name in {"Ge", "Le", "Gt", "Lt", "MultipleOf", "MinLen", "MaxLen"}:
#             key = _pascal_to_snake(name)
#             v = getattr(m, key, None)

#             if v is not None:
#                 out[key] = v

#     return out


# def pydantic_simple_schema(
#     cls: type[BaseModel],
#     *,
#     deep: bool = True,
#     max_depth: int = 5,
# ) -> dict[str, Any]:
#     seen: set[type[BaseModel]] = set()

#     return _recurse_pydantic_model(
#         cls, deep=deep, max_depth=max_depth, depth=0, seen=seen
#     )


# def _pydantic_field_schema(
#     *,
#     name: str,
#     field: FieldInfo,
#     deep: bool,
#     max_depth: int,
#     depth: int,
#     seen: set[type[BaseModel]],
# ) -> dict[str, Any]:
#     nullable, inner = _unwrap_type(field.annotation)
#     t = _base_type(field.annotation)

#     entry: dict[str, Any] = {
#         "type": t,
#         "required": field.is_required(),
#         "nullable": nullable,
#     }

#     entry.update(_constraints(field))

#     if not deep:
#         return entry

#     origin = get_origin(inner)

#     if origin in (list, set, tuple):
#         args = get_args(inner)
#         item_tp = args[0] if args else Any
#         item_nullable, item_inner = _unwrap_type(item_tp)

#         item_entry = {
#             "type": _base_type(item_tp),
#             "nullable": item_nullable,
#         }

#         if _is_pydantic_model(item_inner):
#             item_entry.update(
#                 _recurse_pydantic_model(
#                     item_inner,
#                     deep=True,
#                     max_depth=max_depth,
#                     depth=depth + 1,
#                     seen=seen,
#                 )
#             )

#         entry["items"] = item_entry
#         return entry

#     if origin is dict:
#         args = get_args(inner)
#         if len(args) == 2:
#             value_tp = args[1]
#             v_nullable, v_inner = _unwrap_type(value_tp)
#             value_entry = {"type": _base_type(value_tp), "nullable": v_nullable}
#             if _is_pydantic_model(v_inner):
#                 value_entry.update(
#                     _recurse_pydantic_model(
#                         v_inner,
#                         deep=True,
#                         max_depth=max_depth,
#                         depth=depth + 1,
#                         seen=seen,
#                     )
#                 )
#             entry["values"] = value_entry
#         return entry

#     if _is_pydantic_model(inner):
#         entry.update(
#             _recurse_pydantic_model(
#                 inner,
#                 deep=True,
#                 max_depth=max_depth,
#                 depth=depth + 1,
#                 seen=seen,
#             )
#         )
#         return entry

#     return entry


# def _recurse_pydantic_model(
#     cls: type[BaseModel],
#     *,
#     deep: bool,
#     max_depth: int,
#     depth: int,
#     seen: set[type[BaseModel]],
# ) -> dict[str, Any]:
#     if cls in seen:
#         return {"type": "object", "ref": True}

#     if depth >= max_depth:
#         return {"type": "object", "truncated": True}

#     seen.add(cls)

#     fields_out: dict[str, Any] = {}

#     for name, field in cls.model_fields.items():
#         fields_out[name] = _pydantic_field_schema(
#             name=name,
#             field=field,
#             deep=deep,
#             max_depth=max_depth,
#             depth=depth + 1,
#             seen=seen,
#         )

#     seen.remove(cls)

#     return {"type": "object", "fields": fields_out}
