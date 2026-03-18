"""Merge per-partition histogram ROOT files — ``ydana merge-histos``.

Intended as the manual merge step after a ``fill-histos --per-partition``
run (or to recover from a partially completed job, since existing partition
files are skipped on re-run).  Point this at the directory containing the
per-partition ``*.root`` files and it will sum them into a single ROOT file.

Typical workflow
----------------
::

    # Run fill-histos in per-partition mode:
    ydana fill-histos -i /files/ -o results/ -c 8 --per-partition
    # → results/histograms/histograms_000.root … histograms_NNN.root

    # Job fails? Re-run — existing partition files are skipped automatically.
    # Merge when all partitions are done:
    ydana merge-histos --i results/histograms -o results/
"""

from __future__ import annotations

import glob
import os
import warnings

import uproot
from natsort import natsorted

from ..base.histos import to_root
from ..utils.functions import color_msg, create_outfolder
from ..utils.tqdm import ProgressBarFactory


def _resolve_inputs(inputs: str | list[str]) -> list[str]:
    """Resolve input paths, supporting glob patterns."""
    if isinstance(inputs, str):
        inputs = [inputs]

    resolved = []
    for inp in inputs:
        matched = glob.glob(inp)
        if not matched:
            raise ValueError(f"No files matched {inp!r}")
        else:
            resolved.extend(os.path.abspath(f) for f in matched if f.endswith(".root"))

    return natsorted(resolved)


def merge_histos(
    inputs: str | list[str],
    outfolder: str,
    tag: str = "",
    verbose: bool = True,
) -> None:
    """Merge per-partition histogram ROOT files into a single ROOT file."""
    if verbose:
        color_msg("Running merge-histos...", "green")

    # ── Define output path early so we can protect against it ───────────────
    out_dir = os.path.abspath(os.path.join(outfolder, "histograms"))
    create_outfolder(out_dir)  # Ensure output directory exists
    root_path = os.path.join(out_dir, f"histograms{tag}_merged.root")

    # ── Discover ROOT files ─────────────────────────────────────────────────
    root_files = _resolve_inputs(inputs)

    # Prevent double-counting if the user runs merge twice in the same directory!
    if root_path in root_files:
        warnings.warn(
            f"Output file {root_path!r} found among input files. It will be excluded from merging "
            "to prevent double-counting."
        )
        root_files.remove(root_path)

    if not root_files:
        raise ValueError(f"No ROOT files found in {inputs!r}")

    if verbose:
        color_msg(f"Found {len(root_files)} file(s)", color="blue", indentLevel=1)

    # ── Load and sum histograms key-by-key ──────────────────────────────────
    merged: dict = {}

    desc = color_msg("Merging partitions", "purple", 1, return_str=True)

    # factory in eager mode for a synchronous progress bar
    with ProgressBarFactory(
        mode="eager",
        show=verbose,
        total=len(root_files),
        desc=desc,
        ascii=True,
        unit=" file",
    ) as pbar:
        for path in root_files:
            try:
                with uproot.open(path) as f:
                    for key in f.keys(cycle=False):
                        h = f[key].to_hist()
                        # Sum histograms (or initialize if first time seeing this key)
                        merged[key] = (merged[key] + h) if key in merged else h
            except Exception as exc:
                if verbose:
                    color_msg(
                        f"Skipping corrupt file {path!r}: {exc}",
                        color="yellow",
                        indentLevel=2,
                    )

            pbar.update(1)

    if not merged:
        if verbose:
            color_msg("No valid histograms to merge.", color="red", indentLevel=1)
        return

    if verbose:
        color_msg(
            f"Merged {len(root_files)} file(s) × {len(merged)} histogram(s).",
            color="blue",
            indentLevel=1,
        )

    # ── Write merged ROOT file ──────────────────────────────────────────────
    to_root(merged, root_path)

    if verbose:
        color_msg(f"Histograms saved → {root_path}", color="green", indentLevel=1)
        color_msg("Done!", color="green")
