"""Pydantic-backed implementation of the model codec protocol."""

from collections.abc import Iterator, Sequence
from typing import Literal, cast

import attrs
from pydantic import BaseModel

from ..exceptions import exc
from ..primitives import JsonDict
from .model_codec import ModelCodec, ModelDumpExcludeOptions
from .pydantic import (
    PERSISTENCE_DUMP_EXCLUDE_OPTS,
    pydantic_decode_json_bytes,
    pydantic_dump,
    pydantic_dump_many,
    pydantic_dump_many_batched,
    pydantic_encode_json_bytes,
    pydantic_field_names,
    pydantic_transform,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
)

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class PydanticModelCodec[T: BaseModel](ModelCodec[T, BaseModel]):
    """Model codec that delegates to the Pydantic helper layer."""

    model_type: type[T]  # pyright: ignore[reportIncompatibleMethodOverride]
    """The model type this codec is bound to."""

    materialized: frozenset[str] = frozenset()  # pyright: ignore[reportIncompatibleMethodOverride]
    """``@computed_field`` names persisted (and thus queryable). Empty by default."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.materialized:
            return

        computed = frozenset(self.model_type.model_computed_fields)
        unknown = self.materialized - computed

        if unknown:
            raise exc.configuration(
                f"Materialized field(s) {sorted(unknown)} are not computed fields "
                f"on {self.model_type.__name__}; only ``@computed_field`` members "
                "can be materialized.",
            )

    # ....................... #

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> T:
        return pydantic_validate(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    # ....................... #

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> list[T]:
        return pydantic_validate_many(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    # ....................... #

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> Iterator[list[T]]:
        return pydantic_validate_many_batched(
            self.model_type,
            data,
            batch_size=batch_size,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    # ....................... #

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> JsonDict:
        if exclude is None:
            exclude = {}
        return pydantic_dump(obj, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[JsonDict]:
        if exclude is None:
            exclude = {}
        return pydantic_dump_many(objs, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> Iterator[list[JsonDict]]:
        if exclude is None:
            exclude = {}
        return pydantic_dump_many_batched(
            objs,
            batch_size=batch_size,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform(
        self,
        source: BaseModel,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> T:
        if exclude is None:
            exclude = {"unset": True}
        return pydantic_transform(
            self.model_type,
            source,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform_many(
        self,
        sources: Sequence[BaseModel],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[T]:
        if exclude is None:
            exclude = {"unset": True}
        return pydantic_transform_many(
            self.model_type,
            sources,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def stored_field_names(
        self,
        *,
        include_computed: bool = True,
    ) -> frozenset[str]:
        return pydantic_field_names(
            self.model_type,
            include_computed=include_computed,
        )

    # ....................... #

    def persisted_field_names(self) -> frozenset[str]:
        return pydantic_field_names(self.model_type, include_computed=False) | self.materialized

    # ....................... #

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> bytes:
        if exclude is None:
            exclude = {}
        return pydantic_encode_json_bytes(obj, exclude=exclude)

    # ....................... #

    def encode_persistence_mapping(
        self,
        obj: T,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> JsonDict:
        if exclude is None:
            exclude = {}
        merged = cast(
            ModelDumpExcludeOptions,
            {**PERSISTENCE_DUMP_EXCLUDE_OPTS, **exclude},
        )
        mapping = self.encode_mapping(obj, mode=mode, exclude=merged)

        if self.materialized and merged.get("computed_fields"):
            full = self.encode_mapping(obj, mode=mode, exclude=self._keep_computed(exclude))
            for name in self.materialized:
                if name in full:
                    mapping[name] = full[name]

        return mapping

    # ....................... #

    def encode_persistence_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: ModelDumpExcludeOptions | None = None,
    ) -> list[JsonDict]:
        if exclude is None:
            exclude = {}
        merged = cast(
            ModelDumpExcludeOptions,
            {**PERSISTENCE_DUMP_EXCLUDE_OPTS, **exclude},
        )
        mappings = self.encode_mapping_many(objs, mode=mode, exclude=merged)

        if self.materialized and merged.get("computed_fields"):
            fulls = self.encode_mapping_many(objs, mode=mode, exclude=self._keep_computed(exclude))
            for mapping, full in zip(mappings, fulls, strict=False):
                for name in self.materialized:
                    if name in full:
                        mapping[name] = full[name]

        return mappings

    # ....................... #

    @staticmethod
    def _keep_computed(exclude: ModelDumpExcludeOptions) -> ModelDumpExcludeOptions:
        """The caller's exclude options with computed fields kept in the dump.

        Used to recover the materialized ``@computed_field`` values that the
        persistence dump (which excludes all computed fields) drops, so they can
        be merged back into the stored mapping.
        """

        return cast(ModelDumpExcludeOptions, {**exclude, "computed_fields": False})

    # ....................... #

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T:
        return pydantic_decode_json_bytes(
            self.model_type,
            raw,
            forbid_extra=forbid_extra,
            encoding=encoding,
        )
