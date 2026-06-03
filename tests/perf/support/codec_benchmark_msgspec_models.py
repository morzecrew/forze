"""Msgspec struct mirrors for :mod:`tests.perf.support.codec_benchmark_models` tiers.

Row fixtures are shared (same ``JsonDict`` samples); structs omit Pydantic validators.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import msgspec


class SimpleCodecStruct(msgspec.Struct):
    id: int
    name: str
    value: int


class MediumCodecStruct(msgspec.Struct):
    id: int
    sku: str
    title: str
    active: bool
    price: float
    quantity: int
    owner_id: UUID
    created_at: datetime
    tags: list[str]
    attributes: dict[str, str]
    status: str
    note: str | None = None


class ComplexCodecStruct(msgspec.Struct):
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


class BenchmarkAddressStruct(msgspec.Struct):
    line1: str
    city: str
    region: str
    postal_code: str


class BenchmarkLineItemStruct(msgspec.Struct):
    sku: str
    qty: int
    unit_price: float


class BenchmarkProfileStruct(msgspec.Struct):
    display_name: str
    email: str
    tier: int
    verified: bool


class NestedCodecStruct(msgspec.Struct):
    id: int
    order_code: str
    profile: BenchmarkProfileStruct
    ship_to: BenchmarkAddressStruct
    bill_to: BenchmarkAddressStruct
    lines: list[BenchmarkLineItemStruct]
    tag: str
