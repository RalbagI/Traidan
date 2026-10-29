from collections.abc import Iterable
from typing import Any

from torch import Parameter as Parameter
from torch import Tensor

from . import utils as utils

class Module:
    def __call__(self, state: Tensor) -> Tensor: ...
    def parameters(self) -> Iterable[Parameter]: ...
    def state_dict(self) -> dict[str, Any]: ...
    def load_state_dict(self, state_dict: dict[str, Any]) -> None: ...


class Linear(Module):
    def __init__(self, in_features: int, out_features: int) -> None: ...


class LayerNorm(Module):
    def __init__(self, normalized_shape: int) -> None: ...


class ReLU(Module):
    def __init__(self) -> None: ...


class Sequential(Module):
    def __init__(self, *modules: Module) -> None: ...
