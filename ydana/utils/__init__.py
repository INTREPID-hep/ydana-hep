"""Public utility functions of YDANA."""

__all__ = [
    "color_msg",
    "get_callable_from_src",
    "create_outfolderreconstruct_nested_ids",
]


def __getattr__(name: str) -> object:
    if name == "color_msg":
        from .functions import color_msg

        return color_msg

    if name == "get_callable_from_src":
        from .functions import get_callable_from_src

        return get_callable_from_src

    if name == "create_outfolder":
        from .functions import create_outfolder

        return create_outfolder

    if name == "reconstruct_nested_ids":
        from .functions import reconstruct_nested_ids

        return reconstruct_nested_ids

    raise AttributeError(f"module 'ydana.utils' has no attribute {name!r}")
