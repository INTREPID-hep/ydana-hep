"""Merge per-partition event ROOT files — ``ydana merge-roots``.

Intended as the manual merge step after a ``dump --per-partition``
run (or to recover from a partially completed job, since existing partition
files are skipped on re-run). Point this at the directory containing the
per-partition ``*.root`` files and it will use ROOT's native ``hadd``
utility to concatenate them into a single file.

Typical workflow
----------------
::

    # Run dump in per-partition mode:
    ydana dump -i /files/ -o results/ -c 8 --per-partition --to-root
    # → results/roots/dumpedEvents_000.root … dumpedEvents_NNN.root

    # Merge when all partitions are done:
    ydana merge-dumps -i "results/roots/dumpedEvents_*.root" -o results/
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import warnings

from natsort import natsorted

from ..utils.functions import color_msg, create_outfolder


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


def merge_roots(
    inputs: str | list[str],
    outfolder: str,
    tag: str = "",
    verbose: bool = True,
) -> None:
    """Merge per-partition event ROOT files into a single ROOT file using hadd."""
    if verbose:
        color_msg("Running merge-roots...", "green")

    # ── Verify ROOT environment ─────────────────────────────────────────────
    if shutil.which("hadd") is None:
        raise RuntimeError(
            "The 'hadd' command is not available in your system PATH. "
            "Please ensure ROOT is installed and sourced before running this command."
        )

    # ── Define output path early to protect against double-counting ─────────
    out_dir = os.path.abspath(os.path.join(outfolder, "roots"))
    create_outfolder(out_dir)  # Ensure output directory exists
    root_path = os.path.join(out_dir, f"dumpedEvents{tag}_merged.root")

    # ── Discover ROOT files ─────────────────────────────────────────────────
    root_files = _resolve_inputs(inputs)

    if root_path in root_files:
        warnings.warn(
            f"Output file {root_path!r} found among input files. "
            "It will be excluded from merging to prevent recursive double-counting."
        )
        root_files.remove(root_path)

    if not root_files:
        raise ValueError(f"No ROOT files found matching {inputs!r}")

    if verbose:
        color_msg(f"Found {len(root_files)} file(s)", color="blue", indentLevel=1)
        color_msg("Delegating merge to ROOT's hadd utility...", "purple", indentLevel=1)

    # ── Execute hadd natively ───────────────────────────────────────────────
    # Command: hadd -f output.root input1.root input2.root ...
    cmd = ["hadd", "-f", root_path] + root_files

    try:
        # We capture the output to keep the terminal clean unless it fails
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE if not verbose else None,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        color_msg(
            f"Fatal error: hadd failed with exit code {exc.returncode}",
            "red",
            indentLevel=1,
        )
        print(exc.stderr)
        raise RuntimeError("hadd execution failed. See error output above.") from exc

    if verbose:
        color_msg(f"Merged {len(root_files)} file(s) safely.", color="blue", indentLevel=1)
        color_msg(f"Events saved → {root_path}", "green", indentLevel=1)
        color_msg("Done!", "green")
