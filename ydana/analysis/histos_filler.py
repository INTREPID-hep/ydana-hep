"""Analysis dispatch for the ``ydana fill-histos`` command.

This module acts as the CLI entry-point for histogramming. It loads the
event NTuples, reads histogram definitions from the active config via
:func:`ydana.base.histos.from_config`, and delegates all distributed filling logic
to :func:`ydana.base.histos.fill`.

Output formatting
-----------------
The filled histograms are exported and saved into a ``histograms/`` sub-folder
within the specified output directory.

Per-partition output
--------------------
When ``--per-partition`` is set, each dask partition is evaluated and saved
independently to disk (e.g., ``histograms_{tag}_NNNN.root``).
Existing output files are *skipped* — making the job **resume-safe** if a
failure occurs. Use ``--overwrite`` to force re-processing.

Multiple datasets
-----------------
When ``datasets`` is specified (or when ``inputs`` is omitted and the config
defines ``filesets:``), the processor runs over each dataset independently
and outputs to dataset-specific files:
``histograms_{tag}_{dataset_name}.root``.

Parallelism
-----------
Execution is controlled by ``ncores`` (identical semantics to the dumper):

- ``ncores == 1``   → Always synchronous (single-threaded, easiest to debug).
- ``ncores == -1``  → Dask default scheduler (typically threaded).
- ``ncores > 1``    → Local ``"processes"`` scheduler with *ncores* workers.

If a ``dask.distributed.Client`` is activated transparently at the CLI level
via ``--scheduler-address``, tasks are routed to the cluster and ``ncores``
is ignored (unless ``ncores == 1``).
"""

from __future__ import annotations

from typing import Any, Literal

from ..base import histos as _histos
from ..base.ntuple import NTuple
from ..utils.functions import color_msg


def fill_histos(
    inputs: str | list[str] | dict[str, Any] | None = None,
    outfolder: str = "./results",
    tag: str = "",
    maxfiles: int = -1,
    datasets: str | list[str] | None = None,
    in_format: Literal["root", "parquet"] = "root",
    tree_name: str | list[str] | None = None,
    ncores: int = -1,
    per_partition: bool = False,
    overwrite: bool = False,
    verbose: bool = True,
) -> None:
    """Fill histograms from NTuple files.

    Parameters
    ----------
    inputs : str, list, or None
        Explicit input path(s) forwarded to :class:`~ydana.base.ntuple.NTuple`.
        Mutually exclusive with *datasets*.
        ``None`` → use ``filesets:`` from config.
    outfolder : str
        Output directory.  A ``histograms/`` sub-folder is created inside.
    tag : str
        String appended to the output filename,
        e.g. ``"_v2"`` → ``histograms_v2.root``.
        For multiple datasets the dataset name is also appended:
        ``histograms_v2_DY.root``.
    maxfiles : int
        Cap on number of files loaded per dataset.  ``-1`` = all.
    datasets : str or list[str] or None
        Named datasets from ``filesets:`` in the config.
        ``[]`` / ``None`` (with no *inputs*) → load all filesets.
    in_format : {"root", "parquet"}
        Input file format. ROOT uses coffea.NanoEventsFactory and Parquet uses
        dask_awkward.from_parquet.
    tree_name : str or list[str] or None
        TTree path.  Falls back to config or embedded ``"file.root:treepath"``
        syntax.
    ncores : int
        ``1`` = synchronous, ``-1`` = dask default, ``>1`` = N processes.
    per_partition : bool
        Write one ROOT file per partition (resume-safe).
    overwrite : bool
        Re-process and overwrite existing per-partition files.
    """
    if verbose:
        color_msg("Running fill-histos...", "green")
        color_msg("Loading ntuples", "cyan", indentLevel=0)

    ntuple = NTuple(
        inputs=inputs,
        maxfiles=maxfiles,
        tree_name=tree_name,
        datasets=datasets,
        in_format=in_format,
        verbose=verbose,
    )

    histos = _histos.from_config()

    if not histos:
        if verbose:
            color_msg("No histograms to fill.", color="red", indentLevel=0, bold=True)
        return

    names = [h.name for h in histos]
    limit = 6
    summary = (
        f"{', '.join(names[:limit])} and {len(names) - limit} more…"
        if len(names) > limit
        else ", ".join(names)
    )
    if verbose:
        color_msg(f"Histograms to fill: {summary}", color="yellow", indentLevel=0)

    events_map = ntuple.events if isinstance(ntuple.events, dict) else {"inputs": ntuple.events}

    if len(events_map) > 1:
        if verbose:
            color_msg(
                f"Processing {len(events_map)} dataset(s): {', '.join(events_map)}",
                color="purple",
                indentLevel=0,
            )

    for ds_name, ds_events in events_map.items():
        if verbose:
            color_msg(f"Processing dataset: {ds_name}", color="blue", indentLevel=0)
        _histos.fill(
            histos,
            ds_events,
            outfolder=outfolder,
            tag=f"_{ds_name}{tag}",
            per_partition=per_partition,
            overwrite=overwrite,
            ncores=ncores,
            label=ds_name,
            verbose=verbose,
        )

    if verbose:
        color_msg("Done.", "green")
