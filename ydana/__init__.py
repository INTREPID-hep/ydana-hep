"""Public package interface for YDANA."""

from importlib.metadata import PackageNotFoundError, version

__all__ = [
    "Config",
    "Histogram",
    "NTuple",
    "io",
    "histos",
    "get_run_config",
    "set_run_config",
    "__version__",
]

try:
    __version__ = version("ydana")
except PackageNotFoundError:
    __version__ = "0.1.0"


def __getattr__(name: str) -> object:
    if name == "Config":
        from .base.config import Config

        return Config

    if name == "Histogram":
        from .base.histos import Histogram

        return Histogram
    if name == "NTuple":
        from .base.ntuple import NTuple

        return NTuple

    if name == "io":
        from .base import io

        return io

    if name == "histos":
        from .base import histos

        return histos

    if name == "get_run_config":
        from .base.config import get_run_config

        return get_run_config

    if name == "set_run_config":
        from .base.config import set_run_config

        return set_run_config

    raise AttributeError(f"module 'ydana' has no attribute {name!r}")
