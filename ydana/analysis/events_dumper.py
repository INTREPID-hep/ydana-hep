"""Analysis dispatch for the ``ydana dump-events`` command.

This module acts as the CLI entry-point for exporting event data. It loads
the NTuple, standardizes dataset mappings, and delegates all I/O export
logic to :func:`ydana.utils.io.dump_to_root` or :func:`ydana.utils.io.dump_to_parquet`.

Output formats
--------------
The output format is selected via ``--out-format``:

- **ROOT**: Events are flattened into ``{col}_{field}`` branches. The writer
  attempts a legacy TTree first but automatically falls back to **RNTuple**
  if complex 2D jagged nesting is detected. **One-way export** — nesting
  cannot be recovered from ROOT.
- **Parquet**: The full awkward array is written as-is, preserving all
  nesting levels natively. These files can be reloaded by
  :class:`~ydana.base.ntuple.NTuple` for further processing without loss of
  structural information.

Per-partition output
--------------------
Each dask partition is written independently.
Existing output files are *skipped* — making the job **resume-safe** if a
failure occurs. Use ``--overwrite`` to force re-processing.

Multiple datasets
-----------------
When ``datasets`` is specified (or when ``inputs`` is omitted and the config
defines ``filesets:``), each dataset is processed and dumped into its own
unique subdirectory or file to prevent overwriting.

Parallelism
-----------
Execution is controlled by ``ncores`` (identical semantics to the histogramming):

- ``ncores == 1``   -> Always synchronous (single-threaded, easiest to debug).
- ``ncores == -1``  -> Dask default scheduler (typically threaded).
- ``ncores > 1``    -> Local ``"processes"`` scheduler with *ncores* workers.

If a ``dask.distributed.Client`` is active (via ``--scheduler-address``),
tasks are routed to the cluster and local ``ncores`` hints are ignored.
"""

from __future__ import annotations

from typing import Any, Literal

from ..base.io import dump_to_parquet, dump_to_root
from ..base.ntuple import NTuple
from ..utils.functions import color_msg


def dump_events(
    inputs: str | list[str] | dict[str, Any] | None = None,
    outfolder: str = "./results",
    tag: str = "",
    maxfiles: int = -1,
    datasets: str | list[str] | None = None,
    in_format: Literal["root", "parquet"] = "root",
    tree_name: str | list[str] | None = None,
    ncores: int = -1,
    overwrite: bool = False,
    out_format: Literal["root", "parquet"] = "root",
    verbose: bool = True,
) -> None:
    """Load ntuple files and export the event array to ROOT or Parquet.

    Parameters
    ----------
    inputs : str, list, or None
        Explicit input path(s) forwarded to :class:`~ydana.base.ntuple.NTuple`.
        Mutually exclusive with *datasets*.
    outfolder : str
        Output directory. Created automatically if it does not exist.
    tag : str
        String appended to the output filename.
    maxfiles : int
        Cap on number of files loaded per dataset. ``-1`` = all.
    datasets : str or list[str] or None
        Named datasets from ``filesets:`` in the config.
    in_format : {"root", "parquet"}
        Format of input files used to build the NTuple loader.
    tree_name : str or list[str] or None
        TTree path. Falls back to config or embedded syntax.
    ncores : int
        ``1`` = synchronous, ``-1`` = dask default, ``>1`` = N processes.
    overwrite : bool
        Re-process and overwrite existing per-partition files.
    out_format : {"root", "parquet"}
        Export format for dumped events.
    verbose : bool
        Print console logs and progress bars.

    Raises
    ------
    ValueError
        If either format value is not one of ``"root"`` or ``"parquet"``.
    """
    if in_format not in {"root", "parquet"}:
        raise ValueError("in_format must be either 'root' or 'parquet'.")
    if out_format not in {"root", "parquet"}:
        raise ValueError("out_format must be either 'root' or 'parquet'.")

    fmt = "ROOT" if out_format == "root" else "Parquet"
    if verbose:
        color_msg(f"Running dump events ({fmt})...", "green")
        color_msg("Loading ntuples", "cyan", indentLevel=0)

    ntuple = NTuple(
        inputs=inputs,
        maxfiles=maxfiles,
        tree_name=tree_name,
        datasets=datasets,
        in_format=in_format,
        verbose=verbose,
    )

    events_map = ntuple.events if isinstance(ntuple.events, dict) else {"inputs": ntuple.events}

    if len(events_map) > 1 and verbose:
        color_msg(
            f"Processing {len(events_map)} dataset(s): {', '.join(events_map)}",
            color="purple",
            indentLevel=0,
        )

    for ds_name, ds_events in events_map.items():
        if verbose:
            color_msg(f"Processing dataset: {ds_name}", color="blue", indentLevel=0)

        ds_tag = f"_{ds_name}{tag}" if ds_name != "inputs" else tag

        if out_format == "root":
            dump_to_root(
                events=ds_events,
                outfolder=outfolder,
                treepath="Events/tree",
                tag=ds_tag,
                overwrite=overwrite,
                ncores=ncores,
                label=ds_name,
                verbose=verbose,
            )
        else:
            # ds_outfolder = os.path.join(outfolder, f"parquet{ds_tag}")
            dump_to_parquet(
                events=ds_events,
                outfolder=outfolder,
                tag=ds_tag,
                ncores=ncores,
                overwrite=overwrite,
                label=ds_name,
                verbose=verbose,
            )

    if verbose:
        color_msg("Done.", "green")
