from typing import TypeVar

from pydantic import BaseModel

# ----------------------- #

Q = TypeVar("Q", bound=BaseModel)
P = TypeVar("P", bound=BaseModel)
H = TypeVar("H", bound=BaseModel)
C = TypeVar("C", bound=BaseModel)
B = TypeVar("B", bound=BaseModel)
R = TypeVar("R")
In = TypeVar("In", bound=BaseModel)

Raw = TypeVar("Raw")
"""Operation return value before optional HTTP response projection."""
