"""Minimal PyArrow type stubs for the Traidan intelligence layer.

The pyarrow wheel ships an ``__init__.pyi`` that delegates everything via
``__getattr__(name) -> Any``, which collides with ``[tool.mypy] strict =
true`` plus ``disallow_any_unimported = true``. These stubs cover only the
public surface used by ``intelligence/`` — keep them in sync with the
imports there; expand as needed.
"""

from collections.abc import Iterable, Iterator
from typing import Any

import numpy as np
import numpy.typing as npt

class Field:
    name: str


class DataType:
    num_fields: int
    def __iter__(self) -> Iterator[Field]: ...


class Buffer:
    @property
    def address(self) -> int: ...


class Array:
    @classmethod
    def _import_from_c(cls, array_ptr: int, schema_ptr: int) -> Array: ...
    def _export_to_c(self, array_ptr: int, schema_ptr: int) -> None: ...
    def buffers(self) -> list[Buffer | None]: ...
    @property
    def type(self) -> DataType: ...
    def __len__(self) -> int: ...


class FixedSizeListArray(Array):
    @property
    def values(self) -> Float64Array: ...
    def value_length(self) -> int: ...
    @classmethod
    def from_arrays(cls, values: Array, list_size: int) -> FixedSizeListArray: ...


class Float64Array(Array):
    def to_numpy(self, zero_copy_only: bool = ...) -> npt.NDArray[np.float64]: ...


class StructArray(Array):
    def field(self, name: str) -> Array: ...
    @classmethod
    def from_arrays(
        cls, arrays: list[Array], names: list[str]
    ) -> StructArray: ...


def array(values: Iterable[Any], type: DataType | None = ...) -> Array: ...


def float64() -> DataType: ...
