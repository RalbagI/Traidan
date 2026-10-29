from collections.abc import Iterable

from torch import Parameter

def clip_grad_norm_(parameters: Iterable[Parameter], max_norm: float) -> None: ...
