"""Pydantic models and row fixtures for codec decode performance tiers.

Tiers (increasing cost for strict validation):

- **simple** — few scalar fields, one validator (baseline).
- **medium** — mixed scalars, lists, optional fields, several validators.
- **complex** — ~40 fields, varied types, many validators (stress strict path).
- **nested** — nested models and lists of sub-models (allocation + recursive construct).
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal, NamedTuple
from uuid import UUID

import msgspec
from pydantic import BaseModel, Field, field_validator, model_validator

from forze.base.primitives import JsonDict

from .codec_benchmark_msgspec_models import (
    ComplexCodecStruct,
    MediumCodecStruct,
    NestedCodecStruct,
    SimpleCodecStruct,
)

CodecTierName = Literal["simple", "medium", "complex", "nested"]


class CodecBenchmarkTier(NamedTuple):
    """One benchmark tier: Pydantic + msgspec types and a shared row factory."""

    name: CodecTierName
    pydantic_model: type[BaseModel]
    msgspec_struct: type[msgspec.Struct]
    sample_rows: Any  # Callable[[int], list[JsonDict]]


# ----------------------- #
# Simple (baseline)
# ----------------------- #


class SimpleCodecRow(BaseModel):
    id: int
    name: str
    value: int

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, value: str) -> str:
        if not value.strip():
            msg = "name must be non-empty"
            raise ValueError(msg)

        return value


def sample_simple_rows(n: int) -> list[JsonDict]:
    return [{"id": i, "name": f"row-{i}", "value": i * 2} for i in range(n)]


# ----------------------- #
# Medium
# ----------------------- #


class StockStatus(StrEnum):
    IN_STOCK = "in_stock"
    LOW = "low"
    OUT = "out"


class MediumCodecRow(BaseModel):
    id: int
    sku: str = Field(min_length=3, max_length=32)
    title: str
    active: bool
    price: float = Field(ge=0)
    quantity: int = Field(ge=0)
    owner_id: UUID
    created_at: datetime
    tags: list[str]
    attributes: dict[str, str]
    status: StockStatus
    note: str | None = None

    @field_validator("title")
    @classmethod
    def title_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("title required")

        return value

    @field_validator("tags")
    @classmethod
    def tags_non_empty_strings(cls, value: list[str]) -> list[str]:
        if any(not tag.strip() for tag in value):
            raise ValueError("tags must be non-empty strings")

        return value


def _uuid_for_index(i: int) -> str:
    return str(UUID(int=i + 1))


def sample_medium_rows(n: int) -> list[JsonDict]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows: list[JsonDict] = []

    for i in range(n):
        rows.append(
            {
                "id": i,
                "sku": f"SKU-{i:06d}",
                "title": f"Product {i}",
                "active": i % 3 != 0,
                "price": round(10.5 + (i % 50) * 0.25, 2),
                "quantity": (i % 100) + 1,
                "owner_id": _uuid_for_index(i),
                "created_at": (base.replace(day=1 + (i % 28))).isoformat(),
                "tags": [f"tag-{i}", "benchmark"],
                "attributes": {"color": "blue", "size": str(i % 5)},
                "status": ("in_stock", "low", "out")[i % 3],
                "note": None if i % 4 else f"note {i}",
            }
        )

    return rows


# ----------------------- #
# Complex (~40 fields)
# ----------------------- #


class ComplexCodecRow(BaseModel):
    id: int
    sku: str
    title: str
    subtitle: str | None
    description: str | None
    active: bool
    visible: bool
    price: float
    cost: float
    margin: float
    quantity: int
    reserved_qty: int
    weight_kg: float
    length_cm: float
    width_cm: float
    height_cm: float
    rating: float
    review_count: int
    category_id: int
    brand_id: int
    supplier_id: int
    warehouse_id: int
    owner_uuid: UUID
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None
    locale: str
    currency: str
    tax_code: str
    status: str
    priority: int
    flags: int
    bitmask: int
    version: int
    revision: int
    sort_key: str
    slug: str
    metadata_json: str
    tags: list[str]
    labels: list[str]
    attributes: dict[str, str]
    metrics: dict[str, int]
    scores: list[float]
    dimensions_cm: list[int]
    related_ids: list[int]
    alternate_skus: list[str]
    note: str | None
    internal_note: str | None

    @field_validator("sku", "slug", "sort_key")
    @classmethod
    def strip_required_strings(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("must be non-empty")

        return value

    @field_validator("price", "cost", "margin")
    @classmethod
    def money_non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("money fields must be >= 0")

        return value

    @field_validator("quantity", "reserved_qty", "review_count")
    @classmethod
    def counts_non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("counts must be >= 0")

        return value

    @field_validator("rating")
    @classmethod
    def rating_in_range(cls, value: float) -> float:
        if not 0.0 <= value <= 5.0:
            raise ValueError("rating must be between 0 and 5")

        return value

    @field_validator("tags", "labels")
    @classmethod
    def string_lists_nonempty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("list must not be empty")

        return value

    @model_validator(mode="after")
    def reserved_lte_quantity(self) -> ComplexCodecRow:
        if self.reserved_qty > self.quantity:
            raise ValueError("reserved_qty cannot exceed quantity")

        return self


def sample_complex_rows(n: int) -> list[JsonDict]:
    base = datetime(2023, 6, 1, 12, 0, tzinfo=timezone.utc)
    rows: list[JsonDict] = []

    for i in range(n):
        qty = (i % 200) + 1
        reserved = i % 10
        rows.append(
            {
                "id": i,
                "sku": f"CX-{i:08d}",
                "title": f"Complex item {i}",
                "subtitle": f"Sub {i}" if i % 5 else None,
                "description": f"Description for {i}" if i % 3 else None,
                "active": i % 2 == 0,
                "visible": i % 7 != 0,
                "price": round(99.0 + (i % 40), 2),
                "cost": round(50.0 + (i % 20), 2),
                "margin": round(10.0 + (i % 15), 2),
                "quantity": qty,
                "reserved_qty": reserved,
                "weight_kg": round(0.5 + (i % 30) * 0.1, 2),
                "length_cm": float(10 + (i % 5)),
                "width_cm": float(8 + (i % 4)),
                "height_cm": float(3 + (i % 3)),
                "rating": round((i % 50) / 10.0, 1),
                "review_count": i % 500,
                "category_id": i % 20,
                "brand_id": i % 50,
                "supplier_id": i % 30,
                "warehouse_id": i % 8,
                "owner_uuid": _uuid_for_index(i),
                "created_at": base.isoformat(),
                "updated_at": (base.replace(hour=(i % 24))).isoformat(),
                "published_at": base.isoformat() if i % 4 else None,
                "locale": "en-US",
                "currency": "USD",
                "tax_code": "TAX-A",
                "status": ("draft", "published", "archived")[i % 3],
                "priority": i % 5,
                "flags": i % 256,
                "bitmask": i % 1024,
                "version": 1 + (i % 3),
                "revision": i % 100,
                "sort_key": f"sort-{i:06d}",
                "slug": f"item-{i}",
                "metadata_json": "{}",
                "tags": [f"t{i}", "bench"],
                "labels": ["perf", f"l{i % 10}"],
                "attributes": {"k1": "v1", "tier": "complex"},
                "metrics": {"views": i, "clicks": i % 100},
                "scores": [0.1, 0.2, float(i % 10)],
                "dimensions_cm": [10, 20, 30],
                "related_ids": [i + 1, i + 2],
                "alternate_skus": [f"ALT-{i}"],
                "note": f"n{i}" if i % 6 else None,
                "internal_note": None,
            }
        )

    return rows


# ----------------------- #
# Nested
# ----------------------- #


class BenchmarkAddress(BaseModel):
    line1: str
    city: str
    region: str
    postal_code: str = Field(pattern=r"^\d{5}$")

    @field_validator("line1", "city", "region")
    @classmethod
    def non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("required")

        return value


class BenchmarkLineItem(BaseModel):
    sku: str
    qty: int = Field(ge=1)
    unit_price: float = Field(ge=0)


class BenchmarkProfile(BaseModel):
    display_name: str
    email: str
    tier: int = Field(ge=0, le=3)
    verified: bool


class NestedCodecRow(BaseModel):
    id: int
    order_code: str
    profile: BenchmarkProfile
    ship_to: BenchmarkAddress
    bill_to: BenchmarkAddress
    lines: list[BenchmarkLineItem]
    tag: str

    @field_validator("order_code")
    @classmethod
    def order_code_upper(cls, value: str) -> str:
        code = value.strip().upper()

        if len(code) < 4:
            raise ValueError("order_code too short")

        return code

    @model_validator(mode="after")
    def at_least_one_line(self) -> NestedCodecRow:
        if not self.lines:
            raise ValueError("lines required")

        return self


def sample_nested_rows(n: int) -> list[JsonDict]:
    rows: list[JsonDict] = []

    for i in range(n):
        rows.append(
            {
                "id": i,
                "order_code": f"ORD-{i:05d}",
                "profile": {
                    "display_name": f"User {i}",
                    "email": f"user{i}@example.com",
                    "tier": i % 4,
                    "verified": i % 2 == 0,
                },
                "ship_to": {
                    "line1": f"{100 + i} Ship St",
                    "city": "Bench City",
                    "region": "BC",
                    "postal_code": f"{10000 + (i % 89999):05d}",
                },
                "bill_to": {
                    "line1": f"{200 + i} Bill Ave",
                    "city": "Bench City",
                    "region": "BC",
                    "postal_code": f"{20000 + (i % 69999):05d}",
                },
                "lines": [
                    {
                        "sku": f"L-{i}-A",
                        "qty": 1 + (i % 3),
                        "unit_price": round(5.0 + (i % 20), 2),
                    },
                    {
                        "sku": f"L-{i}-B",
                        "qty": 2,
                        "unit_price": 1.5,
                    },
                ],
                "tag": f"nested-{i}",
            }
        )

    return rows


# ----------------------- #
# Registry
# ----------------------- #

CODEC_BENCHMARK_TIERS: tuple[CodecBenchmarkTier, ...] = (
    CodecBenchmarkTier(
        "simple",
        SimpleCodecRow,
        SimpleCodecStruct,
        sample_simple_rows,
    ),
    CodecBenchmarkTier(
        "medium",
        MediumCodecRow,
        MediumCodecStruct,
        sample_medium_rows,
    ),
    CodecBenchmarkTier(
        "complex",
        ComplexCodecRow,
        ComplexCodecStruct,
        sample_complex_rows,
    ),
    CodecBenchmarkTier(
        "nested",
        NestedCodecRow,
        NestedCodecStruct,
        sample_nested_rows,
    ),
)

TIER_BY_NAME: dict[CodecTierName, CodecBenchmarkTier] = {
    tier.name: tier for tier in CODEC_BENCHMARK_TIERS
}
