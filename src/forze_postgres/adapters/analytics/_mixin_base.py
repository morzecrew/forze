"""Shared mixin base for Postgres analytics type checking."""

from typing import TypeVar, cast

from pydantic import BaseModel

from ._typing_host import PostgresAnalyticsHost

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


class PostgresAnalyticsMixinBase[R: BaseModel, Ing: BaseModel]:
    @property
    def _host(self) -> PostgresAnalyticsHost[R, Ing]:
        return cast(PostgresAnalyticsHost[R, Ing], self)
