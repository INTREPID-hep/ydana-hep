"""Miscelaneous"""

import os
import re
import sys
from functools import lru_cache
from importlib import import_module
from typing import Any, Callable, Optional

import awkward as ak
import dask_awkward as dak
from dask.distributed import get_client


def color_msg(
    msg: str,
    color: Optional[str] = "none",
    indentLevel: Optional[int] = -1,
    return_str: Optional[bool] = False,
    bold: Optional[bool] = False,
    underline: Optional[bool] = False,
    bkg_color: Optional[str] = "none",
) -> Optional[str]:
    """
    Prints a message with ANSI coding so it can be printed with colors.

    :param msg: The message to print.
    :type msg: str
    :param color: The color to use for the message. Default is "none".
    :type color: str
    :param indentLevel: The level of indentation. Default is -1.
    :type indentLevel: int
    :param return_str: If True, returns the formatted message as a string. Default is False.
    :type return_str: bool
    :param bold: If True, makes the text bold. Default is False.
    :type bold: bool
    :param underline: If True, underlines the text. Default is False.
    :type underline: bool
    :param bkg_color: The background color. Default is "none".
    :type bkg_color: str
    :return: The formatted message if return_str is True.
    :rtype: Optional[str]
    """
    style_digit = "0"
    if bold and underline:
        style_digit = "1;4"
    elif bold:
        style_digit = "1"
    elif underline:
        style_digit = "4"

    colors = ["black", "red", "green", "yellow", "blue", "purple", "cyan", "white"]
    font_colors = {color: f";{30 + i}" for i, color in enumerate(colors)}
    background_colors = {color: f";{40 + i}" for i, color in enumerate(colors)}
    font_colors["none"] = ""
    background_colors["none"] = ""

    try:
        ansi_code = f"{style_digit}{font_colors[color]}{background_colors[bkg_color]}m"
    except KeyError:
        ansi_code = f"{style_digit}{font_colors['none']}{background_colors['none']}m"

    indentStr = ""
    if indentLevel == 0:
        indentStr = ">>"
    if indentLevel == 1:
        indentStr = "+"
    if indentLevel == 2:
        indentStr = "*"
    if indentLevel == 3:
        indentStr = "-->"
    if indentLevel >= 4:
        indentStr = "-"

    formatted_msg = "\033[%s%s %s\033[0m" % (
        ansi_code,
        "  " * indentLevel + indentStr,
        msg,
    )

    if return_str:
        return formatted_msg
    else:
        print(formatted_msg)
        return None


def warning_handler(
    message: str,
    category: type,
    filename: str,
    lineno: int,
    file: Optional[Any] = None,
    line: Optional[str] = None,
) -> None:
    """
    Handles warnings by printing them with color formatting.

    :param message: The warning message.
    :type message: str
    :param category: The category of the warning.
    :type category: type
    :param filename: The name of the file where the warning occurred.
    :type filename: str
    :param lineno: The line number where the warning occurred.
    :type lineno: int
    :param file: The file object. Default is None.
    :type file: Any, optional
    :param line: The line of code where the warning occurred. Default is None.
    :type line: str, optional
    """
    print(
        "".join(
            [
                color_msg(
                    f"{category.__name__} in:",
                    color="yellow",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(
                    f"{filename}-{lineno} :",
                    color="purple",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(f"{message}", return_str=True, indentLevel=-1),
            ]
        )
    )


def error_handler(exc_type: type, exc_value: Exception, exc_traceback: Any) -> None:
    """
    Handles errors by printing them with color formatting.

    :param exc_type: The type of the exception.
    :type exc_type: type
    :param exc_value: The exception instance.
    :type exc_value: Exception
    :param exc_traceback: The traceback object.
    :type exc_traceback: Any
    """
    import traceback

    print(
        "".join(
            [
                color_msg(
                    f"{exc_type.__name__}:",
                    color="red",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(f"{exc_value}", color="yellow", return_str=True, indentLevel=-1),
                color_msg(
                    (
                        "Traceback (most recent call last):"
                        + "".join(traceback.format_tb(exc_traceback))
                        if exc_traceback
                        else ""
                    ),
                    return_str=True,
                    indentLevel=-1,
                ),
            ]
        )
    )


@lru_cache(maxsize=256)
def get_callable_from_src(src_str: str) -> Callable:
    """
    Returns the callable object from the given source string.
    Uses a cache to avoid repeated imports of the same callable.

    :param src_str: The source string containing the callable.
    :type src_str: str
    :return: The callable object.
    :rtype: Callable
    """
    if not isinstance(src_str, str):
        raise TypeError(f"src_str must be a string, got {type(src_str).__name__}")

    if "." not in src_str:
        raise ValueError(
            f"Invalid callable path '{src_str}'. Expected dotted path 'module.callable'."
        )

    loaded_callable = None
    try:
        _module_name, _callable_name = src_str.rsplit(".", 1)
        if not _module_name or not _callable_name:
            raise ValueError(
                f"Invalid callable path '{src_str}'. Expected dotted path 'module.callable'."
            )
        _module = import_module(_module_name)
        loaded_callable = getattr(_module, _callable_name)
    except AttributeError as e:
        raise AttributeError(f"{_callable_name} callable not found: {e}")
    except ImportError as e:
        raise ImportError(f"Error importing {src_str}: {e}")

    if not callable(loaded_callable):
        raise TypeError(
            f"Resolved object '{src_str}' is not callable (type: {type(loaded_callable).__name__})."
        )

    return loaded_callable


def make_dask_sched_kwargs(ncores: int) -> dict:
    """Return kwargs to pass to ``dask.compute`` for the requested parallelism.
    Also returns a human-readable label for the chosen scheduler (for logging).

    Priority:

    * ``ncores == 1``  → ``{"scheduler": "synchronous"}`` — always honoured,
      even when a distributed client is active (explicit debug request).
    * ``ncores > 1``   → local ``"processes"`` scheduler **unless** a
      ``dask.distributed.Client`` is already active, in which case the cluster
      takes over and local processes would conflict.
    * ``ncores == -1`` → ``{}`` — defer to whatever is active (distributed
      client if set, otherwise dask's default threaded scheduler).
    """
    if ncores == 1:
        return {"scheduler": "synchronous"}, "synchronous"
    if ncores > 1:
        try:
            get_client()
            return (
                {},
                "distributed",
            )  # distributed client active — let it handle distribution
        except Exception:
            return {
                "scheduler": "processes",
                "num_workers": ncores,
            }, f"processes ({ncores} workers)"
    # ncores == -1: defer to the active scheduler
    return {}, "threaded (dask default)"


# ---------------------------------------------------------------------------
# _is_dak — lazy/eager detection
# ---------------------------------------------------------------------------


def _is_dak(events: object) -> bool:
    """Return ``True`` when *events* is a ``dask_awkward.Array``."""
    try:
        return isinstance(events, dak.Array)
    except ImportError:
        return False


def create_outfolder(outname: str) -> None:
    """
    Creates an output directory if it does not exist.

    :param outname: The path of the output directory.
    :type outname: str
    """
    if not isinstance(outname, str) or not outname:
        raise ValueError("Output folder path must be a non-empty string.")
    if os.path.isfile(outname):
        color_msg(
            f"Warning: '{outname}' exists and is a file, not a directory.",
            color="yellow",
        )
        raise FileExistsError(f"Cannot create directory '{outname}': a file with that name exists.")
    try:
        os.makedirs(outname, exist_ok=True)
    except Exception as e:
        color_msg(f"Failed to create directory '{outname}': {e}", color="red")
        raise


def ensure_on_syspaths(path: str | list[str]) -> None:
    """Add the paths to ``sys.path`` (idempotently)."""
    paths = path if isinstance(path, list) else [path]
    for dir in paths:
        if dir not in sys.path:
            sys.path.insert(0, dir)


def find_field_by_pattern(fields: list[str], pattern: re.Pattern) -> str | None:
    """Return the first field in *fields* whose name matches *pattern*, or None."""
    return next((f for f in fields if pattern.search(f)), None)


# ---------------------------------------------------------------------------
# Nested-ids reconstruction preprocessor factory
# ---------------------------------------------------------------------------


def reconstruct_nested_ids(
    flat_field: str,
    n_field: str,
    col: str,
    out_field: str | None = None,
) -> Callable:
    """Factory: return a preprocessor that rebuilds a doubly-jagged ids field.

    A ROOT TTree (or any data source using the flat+count convention) encodes
    a ``var * var * int`` field as two **top-level** branches on the events
    array:

    * ``events[flat_field]``  — ``var * int``, all ids for an event concatenated
      across all parent particles (e.g. ``[[5], [2, 7]]``).
    * ``events[n_field]``     — ``var * int``, number of ids contributed by each
      parent particle in that event (e.g. ``[[1, 0], [2]]``).

    The returned preprocessor reconstructs the original ``var * var * int``
    field via ``ak.unflatten(flat, counts, axis=1)`` and injects it into the
    *col* collection under the name *out_field*.

    Parameters
    ----------
    flat_field : str
        Top-level events field with the flat per-event ids,
        e.g. ``"tps_matched_showers_ids"``  (``var * int``).
    n_field : str
        Top-level events field with the per-parent-particle counts,
        e.g. ``"tps_matched_showers_ids_n"``  (``var * int``).
    col : str
        Name of the collection to which the nested field is added,
        e.g. ``"tps"``.
    out_field : str, optional
        Name of the new nested field within *col*.  Defaults to
        *flat_field* with the trailing ``_ids`` suffix stripped,
        e.g. ``"tps_matched_showers"`` when *flat_field* is
        ``"tps_matched_showers_ids"``.

    Returns
    -------
    Callable
        A preprocessor ``fn(events) -> None`` that mutates *events* in-place.

    Examples
    --------
    Use in a YAML pre-steps block:

    .. code-block:: yaml

       pre-steps:
         - name: reconstruct_nested_ids
           args:
             - "tps_matched_showers_ids"
             - "tps_matched_showers_ids_n"
             - "tps"

    Or programmatically::

        from ydana.utils.preprocessors import reconstruct_nested_ids

        pp = reconstruct_nested_ids(
            "tps_matched_showers_ids",
            "tps_matched_showers_ids_n",
            "tps",
        )
        pp(events)
        # events["tps"]["matched_showers_ids"] is now var * var * int
    """
    # Infer the output field name if not provided.
    # Strip the trailing "_ids" suffix so e.g. "tps_matched_showers_ids" → "tps_matched_showers".
    resolved_out = (
        out_field
        if out_field is not None
        else (flat_field[:-4] if flat_field.endswith("_ids") else flat_field)
    )

    def _preprocessor(events: ak.Array | dak.Array) -> None:
        flat_ids = events[flat_field]  # var * int per event
        counts = events[n_field]  # var * int per event (count per parent)
        # ak.unflatten requires 1-D counts.  We have jagged counts (one per TP
        # per event), so we need a two-step unflatten:
        #   1. flatten everything to 1D and rebuild per-TP lists
        #   2. group the per-TP lists back into per-event lists
        flat_all = ak.flatten(flat_ids)  # 1-D
        per_tp_counts = ak.flatten(counts)  # 1-D
        n_tps_per_event = ak.num(counts, axis=1)  # 1-D
        per_tp_ids = ak.unflatten(flat_all, per_tp_counts)  # var * int
        nested = ak.unflatten(per_tp_ids, n_tps_per_event)  # var * var * int
        events[col] = ak.with_field(events[col], nested, resolved_out)

    _preprocessor.__name__ = (
        f"reconstruct_nested_ids({flat_field!r}, {n_field!r} → {col!r}.{resolved_out!r})"
    )
    _preprocessor.__qualname__ = _preprocessor.__name__
    return _preprocessor
