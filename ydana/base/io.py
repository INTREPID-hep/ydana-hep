import os
import warnings

import awkward as ak
import dask
import dask_awkward as dak
import uproot

from ..utils.functions import (
    _is_dak,
    color_msg,
    create_outfolder,
    make_dask_sched_kwargs,
)
from ..utils.tqdm import ProgressBarFactory
from .particle import ParticleRecord


def dump_to_parquet(
    events: ak.Array | dak.Array,
    outfolder: str,
    tag: str = "",
    ncores: int = -1,
    overwrite: bool = False,
    label: str = "",
    verbose: bool = True,
) -> None:
    """Persist the full event array to Parquet, preserving all nesting.

    Unlike ROOT flat NTuples, Parquet natively supports Awkward Array's
    jagged and nested data structures. The output written by this function
    preserves all cross-references and collections natively, and can be
    reloaded seamlessly by the framework without losing structural information.

    Parameters
    ----------
    events : ak.Array or dak.Array
        The event array to persist. The type determines the output structure:

        - ``ak.Array`` (Eager): Writes a single file to
          ``<outfolder>/dumpedEvents.parquet``.
        - ``dak.Array`` (Lazy): Writes a partitioned dataset directly into
          ``<outfolder>/`` with partitioned files (e.g.,
          ``dumpedEvents-part0.parquet``).
    outfolder : str
        Output directory where the Parquet file(s) will be saved. Created
        automatically if it does not exist.
    tag : str, optional
        Suffix appended to the output filename stem.
    ncores : int, optional
        Dask scheduler hint for lazy arrays. ``1`` = synchronous (debug),
        ``-1`` = dask default, ``>1`` = N local process workers.
        Ignored for eager arrays.
    overwrite : bool, optional
        If ``False`` (default), skips writing if the target file already
        exists.
    label : str, optional
        Human-readable label used in log messages (e.g., dataset name).
    verbose : bool, optional
        Whether to print log messages and display the progress bar. Defaults
        to ``True``.

    Notes
    -----
    **Lazy Execution (Dask):**
    When passed a ``dask_awkward.Array``, this function bypasses the default
    blocking behavior of ``dak.to_parquet`` by passing ``compute=False``.
    This allows the framework to execute delayed write operations using the
    specified scheduler environment (``ncores``).
    """
    # ── 1. Sanity Checks ────────────────────────────────────────────────────
    if events is None:
        raise ValueError("No events provided to dump.")

    if len(ak.fields(events)) == 0:
        raise ValueError("Events array has no columns/fields.")

    _is_eager = False
    if not _is_dak(events):
        if len(events) == 0:
            warnings.warn("Events array is empty (0 rows). Skipping dump.", stacklevel=2)
            return
        _is_eager = True
    # ────────────────────────────────────────────────────────────────────────

    out_dir = os.path.join(outfolder, "parquets")
    create_outfolder(out_dir)

    log_label = f"[dump|{label}]" if label else "[dump]"

    # Ensure parent dirs exist
    create_outfolder(out_dir)

    # ── Eager Path ──────────────────────────────────────────────────────────
    if _is_eager:
        outpath = os.path.join(out_dir, f"dumpedEvents{tag}.parquet")
        if not overwrite and os.path.exists(outpath):
            if verbose:
                color_msg(
                    f"{log_label} Skipping existing {outpath} (resume-safe)",
                    "yellow",
                    1,
                )
            return
        if verbose:
            color_msg(f"{log_label} Writing eager array to Parquet...", "purple", 1)
        ak.to_parquet(events, outpath)
        if verbose:
            color_msg(f"{log_label} Parquet saved → {outpath}", "green", 1)
        return

    # ── Lazy Path ───────────────────────────────────────────────────────────
    sched_kwargs, sched_label = make_dask_sched_kwargs(ncores)
    npartitions = events.npartitions

    if verbose:
        color_msg(
            f"{log_label} Parquet export × {npartitions} partition(s) | {sched_label}",
            "blue",
            1,
        )

    # By passing compute=False, we get the Delayed object instead of blocking!
    write_task = dak.to_parquet(events, out_dir, prefix=f"dumpedEvents{tag}", compute=False)

    desc = color_msg(f"{log_label} Writing to Parquet", "purple", 1, return_str=True)

    with ProgressBarFactory(
        mode="lazy", show=verbose, delay=0.25, desc=desc, ascii=True, unit=" part"
    ):
        dask.compute(write_task, **sched_kwargs)

    if verbose:
        color_msg(f"{log_label} Parquet saved → {out_dir}", "green", 1)


def _flatten_awkward_to_dict(
    array: ak.Array | dak.Array,
    prefix: str = "",
    depth: int = 0,
) -> dict[str, ak.Array | dak.Array]:
    """Recursively flatten an Awkward array into a dictionary of 1D columns.

    Parameters
    ----------
    array : ak.Array or dak.Array
        The array (or sub-array) to flatten.
    prefix : str, optional
        The accumulated branch name from parent records (e.g., "muons_matched").
    depth : int, optional
        Current recursion depth. 0 = top-level events, 1 = main collections, etc.

    Returns
    -------
    dict
        A dictionary mapping flattened branch names (strings) to Awkward arrays.
    """
    branches = {}

    for field in ak.fields(array):
        col = array[field]
        subfields = ak.fields(col)

        # Build the new branch name (e.g., "muons" + "pt" -> "muons_pt")
        new_name = f"{prefix}_{field}" if prefix else field

        if not subfields:
            # Base Case: It's a plain scalar or 1D jagged array (leaf node)
            _new_name = "event_" + new_name if depth == 0 else new_name
            branches[_new_name] = col

        else:
            # Recursive Case: It's a collection / record

            # If already are inside a collection (depth > 0), extract
            # IDs instead of fully expanding, to prevent massive ROOT file bloat.
            if depth > 0 and hasattr(ParticleRecord, "ids_from_array"):
                branches[f"{new_name}_ids"] = ParticleRecord.ids_from_array(col)
            else:
                # Top-level collections (depth 0) OR fallback if ID extraction is missing.
                branches.update(_flatten_awkward_to_dict(col, prefix=new_name, depth=depth + 1))

    return branches


@dask.delayed
def _write_root_partition(
    partition: ak.Array,
    out_path: str,
    treepath: str,
) -> None:
    """Delayed worker to write one materialized partition to a ROOT file.

    This is a ``@dask.delayed`` worker called by :func:`dump_to_root` in
    per-partition mode. It receives one fully computed ``awkward.Array``
    partition, creates a *flattened* representation of the event array,
    and writes it into a TTree (or RNTuple fallback).

    Parameters
    ----------
    partition : ak.Array
        The materialized awkward array chunk.
    out_path : str
        The full destination path for the ``.root`` file.
    treepath : str
        The name of the Tree inside the ROOT file.
    """
    branches = _flatten_awkward_to_dict(partition)
    if not branches:
        return
    with uproot.recreate(out_path) as f:
        try:
            f.mktree(treepath, branches)
            f[treepath].extend(branches)
        except TypeError:
            warnings.warn(
                "Jaggedness or unsupported types for standard TTree detected. "
                "Automatically falling back to RNTuple format.",
                stacklevel=2,
            )
            f[treepath] = branches


def dump_to_root(
    events: ak.Array | dak.Array,
    outfolder: str,
    treepath: str = "Events/tree",
    tag: str = "",
    overwrite: bool = False,
    ncores: int = -1,
    label: str = "",
    verbose: bool = True,
) -> None:
    """Write an event array to a new ROOT file. For dask like arrays, each partition is written to a
    separate file (e.g., ``dumpedEvents_000.root``).

    .. warning:: **One-way export only.**
       The output is a *flattened* representation of the event array — nested
       particle collections are decomposed into ``{col}_{field}`` branches.
       The nesting information (e.g., that ``tps_matched_showers_ids`` encodes
       cross-references into the ``showers`` collection) is not preserved in the
       ROOT file. If you need to **persist the full array structure for later
       re-use**, use :func:`dump_to_parquet` instead.

    Parameters
    ----------
    events : ak.Array or dak.Array
        The event array to persist. If a lazy dask-awkward array is passed alongside
        ``per_partition=False``, it will be materialized into memory, which may
        cause Out-Of-Memory errors for large datasets.
    outfolder : str
        Output directory where the ROOT file(s) will be saved.
    treepath : str, optional
        The path and name of the Tree inside the ROOT file. Default is ``"Events/tree"``.
    tag : str, optional
        Suffix appended to the output filename stem.
    overwrite : bool, optional
        If ``False`` (default), skips writing if the target file already exists.
    ncores : int, optional
        Dask scheduler hint for lazy arrays.
    label : str, optional
        Human-readable label used in log messages.
    verbose : bool, optional
        Whether to print log messages and display the progress bar.

    Notes
    -----
    **Branch layout & Fallback:**
    The writer dynamically attempts to create a standard ``TTree`` for maximum
    compatibility. If the event array contains complex 2D jagged nesting
    (``var * var * type``), it will safely intercept the error and write an
    ``RNTuple`` instead.
    """
    # ── 1. Sanity Checks ────────────────────────────────────────────────────
    if events is None:
        raise ValueError("No events provided to dump.")

    if len(ak.fields(events)) == 0:
        raise ValueError("Events array has no columns/fields.")

    _is_eager = False
    if not _is_dak(events):
        if len(events) == 0:
            warnings.warn("Events array is empty (0 rows). Skipping dump.", stacklevel=2)
            return
        _is_eager = True
    # ────────────────────────────────────────────────────────────────────────

    out_dir = os.path.join(outfolder, "roots")
    create_outfolder(out_dir)

    log_label = f"[dump|{label}]" if label else "[dump]"

    # ------------------------------------------------------------------
    # Eager path — ak.Array (no Dask at all)
    # ------------------------------------------------------------------
    if _is_eager:
        out_path = os.path.join(out_dir, f"dumpedEvents{tag}.root")
        if not overwrite and os.path.exists(out_path):
            if verbose:
                color_msg(f"{log_label} Skipping existing {out_path}", "yellow", 1)
            return
        if verbose:
            color_msg(f"{log_label} Flattening and writing to ROOT...", "purple", 1)

        # Compute is needed here to un-delay the worker for eager arrays
        _write_root_partition(events, out_path, treepath).compute()

        if verbose:
            color_msg(f"{log_label} ROOT saved → {out_dir}", "green", 1)
        return

    # ------------------------------------------------------------------
    # Lazy path — dak.Array
    # ------------------------------------------------------------------

    # -- Per-Partition ROOT Files --
    sched_kwargs, sched_label = make_dask_sched_kwargs(ncores)
    npartitions = events.npartitions
    pad = len(str(max(npartitions - 1, 0)))

    if verbose:
        color_msg(
            f"{log_label} ROOT export × {npartitions} partition(s) | {sched_label}",
            "blue",
            1,
        )

    tasks = []
    for i, part in enumerate(events.partitions):
        out_path = os.path.join(out_dir, f"dumpedEvents{tag}_{str(i).zfill(pad)}.root")
        if not overwrite and os.path.exists(out_path):
            if verbose:
                color_msg(f"{log_label} Skipping {out_path} (resume-safe)", "yellow", 1)
            continue
        tasks.append(_write_root_partition(part, out_path, treepath))

    if not tasks:
        if verbose:
            color_msg(f"{log_label} All partition files exist. Nothing to do.", "green", 1)
        return

    desc = color_msg(f"{log_label} Writing ROOT partitions", "purple", 1, return_str=True)
    with ProgressBarFactory(
        mode="lazy", show=verbose, delay=0.25, desc=desc, ascii=True, unit=" node"
    ):
        dask.compute(*tasks, **sched_kwargs)

    if verbose:
        color_msg(f"{log_label} ROOT partitions saved → {out_dir}", "green", 1)
