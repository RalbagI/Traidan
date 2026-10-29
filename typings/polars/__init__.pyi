from collections.abc import Iterable
from typing import Any

import numpy as np
import numpy.typing as npt

class DataFrame:
    columns: list[str]
    height: int
    def __init__(self, data: dict[str, Iterable[Any]] | None = ...) -> None: ...
    def __getitem__(self, name: str) -> Series: ...


class Series:
    name: str
    def __init__(
        self,
        name: str,
        values: Iterable[Any] | npt.NDArray[np.generic],
    ) -> None: ...
    def __getitem__(self, idx: int) -> Any: ...
    def to_numpy(self) -> npt.NDArray[np.generic]: ...
