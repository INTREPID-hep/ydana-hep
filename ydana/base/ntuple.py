"""
NTuple — file loader for ROOT and Parquet files using coffea and dask-awkward.

Overview:
---------
ROOT (.root):
    - Parses schema from config or per-dataset key.
    - Optionally partitions fileset if step_size is set.
    - Loads events lazily with NanoEventsFactory (dask mode).
    - Injects constant fields from schema.
    - Executes pre-steps pipeline.

Parquet (.parquet):
    - Loads lazily with dask_awkward.from_parquet.
    - Executes pre-steps pipeline.

Input resolution:
    - inputs: explicit files → dak.Array
    - datasets: named filesets from config → dict[str, dak.Array]
    - neither: loads all filesets from config → dict[str, dak.Array]

Multiple datasets:
    - events is dict[str, dak.Array], keeping datasets separate.

Per-dataset config keys:
    - treename: tree path
    - schema: schema override
    - step_size: entries per partition
    - split_row_groups: Parquet row-group splitting
    - metadata: arbitrary dict attached to ntuple.metadata[name]

tree_name in constructor is a global override, can be a list[str] matching datasets.

Public usage:
    ntuple = NTuple("/path/to/dir/", tree_name="Events")
    ntuple = NTuple(datasets=["DY", "Zprime"])
    ntuple = NTuple()

    ntuple.events                            # dak.Array or dict[str, dak.Array]
    ntuple.events["digis"]["BX"].compute()   # single dataset
    ntuple.events["DY"]["digis"]["BX"].compute()  # multi-dataset

Input formats:
    - ``file.root``                              single ROOT file
    - ``/dir/*.root``                            glob pattern
    - ``/dir/``                                  directory (finds ROOT or Parquet files)
    - ``["a.root", "b.root"]``                 list of files
    - ``{"file.root": "tree", ...}``          dict (uproot-native)
    - ``{"file.root": {"object_path": "ttree_v1", "steps": [[0, 10000], [15000, 20000], ...]}}``
    - ``output.parquet``                         single Parquet file
    - ``["a.parquet", "b.parquet"]``           list of Parquet files
"""

from __future__ import annotations

import glob as _glob
import os
from pathlib import Path
from typing import Literal, TypeAlias

import dask_awkward as dak
from coffea.nanoevents import NanoEventsFactory
from natsort import natsorted

from ..utils.functions import color_msg, ensure_on_syspaths
from .config import Config, get_run_config
from .pipeline import execute_pipeline
from .schema import YAMLSchema, _branches_from_schema, _inject_constants

FormattedFiles: TypeAlias = dict[str, str | None]
FileSpec: TypeAlias = str | Path | list[object] | dict[str, object]
EventsMap: TypeAlias = dict[str, dak.Array]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_input(
    files: FileSpec,
    tree_path: str | None,
    max_files: int = -1,
    base_dir: str | None = None,
    in_format: Literal["root", "parquet"] = "root",
) -> FormattedFiles:
    """
    Normalize files into an uproot-native {file: tree_path} dict.
    For parquet inputs, tree_path is always None and ignored.
    """
    # ── dict: {filepath: tree_spec} ──────────────────────────────────────────
    if isinstance(files, dict):
        result = {}
        for file_path, tree_spec in files.items():
            effective_tree = tree_path if tree_path is not None else tree_spec
            # Recursion: pass all flags, but disable max_files locally to slice at the end
            result.update(
                _format_input(
                    file_path,
                    effective_tree,
                    max_files=-1,
                    base_dir=base_dir,
                    in_format=in_format,
                )
            )
        return dict(list(result.items())[:max_files]) if max_files > 0 else result

    # ── list: [filepath, ...] or [{"file": "tree"}, ...] ───────────────────
    if isinstance(files, list):
        result = {}
        for item in files:
            result.update(
                _format_input(
                    item,
                    tree_path,
                    max_files=-1,
                    base_dir=base_dir,
                    in_format=in_format,
                )
            )
        return dict(list(result.items())[:max_files]) if max_files > 0 else result

    # ── str: single path, glob pattern, Path, or "file.root:tree" ──────────
    if isinstance(files, (str, Path)):
        files_str = str(files)
        effective_tree = tree_path

        # 1. Strip embedded tree BEFORE path resolution
        if ":" in files_str and in_format == "root":
            files_str, embedded_tree = files_str.rsplit(":", 1)
            effective_tree = tree_path if tree_path is not None else embedded_tree

        if not effective_tree and in_format == "root":
            raise ValueError(
                "No tree name provided. Supply it via --tree TREEPATH, "
                "fileset 'treename:' key, or embed it in the file path 'file.root:treepath'."
            )

        # 2. Resolve absolute path
        _base_dir = os.getcwd() if base_dir is None else os.path.abspath(base_dir)
        absolute_path = os.path.abspath(os.path.join(_base_dir, files_str))

        # 3. Handle directories
        if os.path.isdir(absolute_path):
            ext = "*.root" if in_format == "root" else "*.parquet"
            absolute_path = os.path.join(absolute_path, ext)

        # 4. Glob expansion
        file_list = natsorted(_glob.glob(absolute_path))

        if not file_list:
            ext_name = ".root" if in_format == "root" else ".parquet"
            raise FileNotFoundError(f"No {ext_name} file(s) found at {absolute_path}")

        # If a glob expands to multiple files, process them as a list to ensure flattening
        if len(file_list) > 1:
            return _format_input(
                file_list,
                effective_tree,
                max_files=max_files,
                base_dir=_base_dir,
                in_format=in_format,
            )

        # Single resolved file
        return {file_list[0]: effective_tree if in_format == "root" else None}


def _resolve_schema(schema_section: object) -> tuple[object, type, dict[str, set[str]]]:
    """
    Resolve a raw schema value into (schema_section, schema_cls, uproot_opts).
    schema_section: None, str, or dict
        - None: coffea BaseSchema (reads all branches)
        - str: named coffea schema class / config section (e.g. "SchemaA" or "MySchema")
        - dict: YAMLSchema config (branch allow-list + constants)
    """
    uproot_opts: dict = {}

    if schema_section is None:
        from coffea.nanoevents.schemas.base import BaseSchema

        schema_cls = BaseSchema
    elif isinstance(schema_section, str):
        import coffea.nanoevents.schemas as _schemas

        schema_cls = getattr(_schemas, schema_section, None)
        if schema_cls is None:
            raise ValueError(
                f"Unknown coffea schema class: '{schema_section}'. "
                "Check coffea.nanoevents.schemas for available names."
            )
    else:
        allowed = _branches_from_schema(schema_section)
        uproot_opts = {"filter_name": allowed}
        schema_cls = YAMLSchema.with_config(schema_section)

    return schema_section, schema_cls, uproot_opts


def _partition_inputs(files: str | FormattedFiles, step_size: int) -> str | FormattedFiles:
    """
    Partition ROOT files using coffea.dataset_tools.preprocess.
    Converts {file: treepath} dict into a coffea fileset, discovers entry counts,
    computes chunk boundaries, and returns the preprocessed files dict.
    """
    from coffea.dataset_tools import preprocess

    # Normalise into a {file: {object_path: ...}} dict for coffea
    if isinstance(files, str):
        # "path/to/file.root:treepath" form
        file_part, obj_path = files.rsplit(":", 1)
        coffea_files = {file_part: {"object_path": obj_path}}
    elif isinstance(files, dict):
        coffea_files = {fpath: {"object_path": tree} for fpath, tree in files.items()}
    else:
        return files  # pass through unknown forms

    fileset = {"_dataset": {"files": coffea_files}}
    available, _ = preprocess(fileset, step_size=step_size)

    return available["_dataset"]["files"]


def _load_from_root(
    formatted_files: FormattedFiles,
    config: Config,
    step_size: int | None = None,
    schema_section: object = None,
) -> dak.Array:
    """
    Build a lazy dask-awkward graph from a formatted ROOT fileset.
    formatted_files: {abs_path: treepath} dict from _format_input.
    schema_section: schema override (None falls back to config.Schema).
    step_size: partitions files if set.
    """
    if schema_section is not None:
        # str → look up as named schema in config first, else treat as coffea class name
        # dict → use directly as YAMLSchema config
        effective_schema = (
            getattr(config, schema_section, None) or schema_section
            if isinstance(schema_section, str)
            else schema_section
        )
    else:
        effective_schema = getattr(config, "Schema", None)  # global fallback; None → BaseSchema

    schema_sec, schema_cls, uproot_opts = _resolve_schema(effective_schema)

    load_files = formatted_files
    if step_size is not None:
        load_files = _partition_inputs(formatted_files, step_size)

    events = NanoEventsFactory.from_root(
        load_files,
        schemaclass=schema_cls,
        mode="dask",
        uproot_options=uproot_opts,
    ).events()

    if isinstance(schema_sec, dict):
        events = _inject_constants(events, schema_sec)

    return events


# ---------------------------------------------------------------------------
# Fileset inspector
# ---------------------------------------------------------------------------


def _print_files_block(loaded_files: dict, ds_treename: str = "", max_files: int = 3) -> None:
    """
    Render the indented file listing for one dataset block.
    Shows up to max_files, with tree name annotation if different from dataset tree.
    """
    if not loaded_files:
        return

    items = list(loaded_files.items())
    show = items if max_files < 0 else items[:max_files]
    hidden = len(items) - len(show)

    for i, (path, file_tree) in enumerate(show):
        # If there are hidden files, the last visible file still gets a "├"
        # because the "└" is reserved for the "+X more" line.
        is_last = (i == len(show) - 1) and hidden == 0
        connector = "└" if is_last else "├"

        fname = os.path.basename(path)

        # Only show the tree name if it differs from the parent dataset tree
        tree_part = ""
        if file_tree and file_tree != ds_treename:
            tree_part = color_msg(f"  →  {file_tree}", "purple", return_str=True)

        print(
            color_msg(f"      {connector}  ", "cyan", return_str=True)
            + color_msg(fname, "white", return_str=True)
            + tree_part
        )

    if hidden > 0:
        color_msg(f"      └  … +{hidden} more", "cyan")


def _print_dataset_block(
    ds_name: str,
    loaded_files: dict,
    npart: int | str,
    ds_cfg: dict | None = None,
    max_files: int = 3,
    indent: int = 0,
) -> None:
    """
    Print the header, info line, and file listing for one dataset or input block.
    Shows dataset name, file count, config info, file listing, and partition count.
    """
    ds_cfg = ds_cfg or {}
    meta = ds_cfg.get("metadata", {})

    # Resolve tree name for display
    ds_treename = ds_cfg.get("treename") or ds_cfg.get("tree_name") or ""
    if not ds_treename and loaded_files:
        trees = {t for t in loaded_files.values() if t}
        if len(trees) == 1:
            ds_treename = next(iter(trees))

    n = len(loaded_files)
    label = meta.get("label", "")
    version = meta.get("version", "")
    tag = (f"  {label}" if label else "") + (f"  [v{version}]" if version else "")

    # 1. Print Header
    icon = "  ▸ " if ds_name != "inputs" else "▸ "
    print(
        color_msg(icon, "cyan", return_str=True, indentLevel=indent)
        + color_msg(ds_name, "yellow", bold=True, return_str=True)
        + color_msg(f"  ({n} file{'s' if n != 1 else ''})", "white", return_str=True)
        + color_msg(tag, "white", return_str=True)
    )

    # 2. Print Config Info (cleaner list construction)
    info_parts = []
    if ds_treename:
        info_parts.append(f"tree: {ds_treename}")
    if ds_cfg.get("step_size"):
        info_parts.append(f"step_size={ds_cfg['step_size']}")
    if ds_cfg.get("split_row_groups"):
        info_parts.append("split_row_groups=True")

    if info_parts:
        color_msg("      " + "  ·  ".join(info_parts), "purple", indentLevel=indent)

    # 3. Print Files and Partitions
    _print_files_block(loaded_files, ds_treename, max_files)

    # Adjust indentation based on whether this is a nested dataset or the root input
    part_indent = indent + 3 if ds_name != "inputs" else indent
    color_msg(f"      {npart} partition(s)", "green", indentLevel=part_indent)


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------


class NTuple:
    """
    NTuple loader for ROOT and Parquet files.

    Loads ROOT files lazily via coffea.NanoEventsFactory or Parquet files via 
    dask_awkward.from_parquet.

    Input resolution:
        - inputs: explicit files → dak.Array
        - datasets: named filesets from config → dict[str, dak.Array]
        - neither: loads all filesets from config → dict[str, dak.Array]

    Multiple datasets:
        - events is dict[str, dak.Array], keeping datasets separate.

    Per-dataset config keys:
        - treename: tree path
        - schema: schema override
        - step_size: entries per partition
        - split_row_groups: Parquet row-group splitting
        - metadata: arbitrary dict attached to ntuple.metadata[name]

    Parameters:
        inputs: str, list, dict, or None (explicit input path, mutually exclusive with datasets)
        maxfiles: int (cap on files per dataset, -1 = all)
        datasets: str, list[str], or None (named datasets to load, [] = all filesets)
        tree_name: str | list[str] | None (TTree path, str = same for all, list = one per dataset)
        step_size: int (ROOT: entries per partition)
        split_row_groups: bool (Parquet: one partition per row group)
        in_format: {"root", "parquet"} (input file format)
        CONFIG: Config (defaults to :func:`~ydana.base.config.get_run_config`)
        verbose: bool (print info summary)

    Attributes:
        events: dak.Array or dict[str, dak.Array] (single lazy array or per-dataset mapping)
        metadata: dict or dict[str, dict] (flat dict or per-dataset mapping)
    """

    def __init__(
        self,
        inputs: str | list | dict | None = None,
        maxfiles: int = -1,
        datasets: str | list[str] | None = None,
        tree_name: str | list[str] | None = None,
        step_size: int | None = None,
        split_row_groups: bool | None = None,
        in_format: Literal["root", "parquet"] = "root",
        CONFIG: Config | None = None,
        verbose: bool = True,
    ) -> None:
        if in_format not in {"root", "parquet"}:
            raise ValueError("in_format must be either 'root' or 'parquet'.")

        # check this if logic..
        if inputs is not None and datasets is not None:
            raise ValueError("'inputs' and 'datasets' are mutually exclusive.")

        self.CONFIG = CONFIG if CONFIG is not None else get_run_config()

        # sanity paths
        ensure_on_syspaths([self.CONFIG.path, os.getcwd()])

        self._maxfiles = maxfiles

        self.events: EventsMap | dak.Array = {}
        self.metadata: dict[str, dict] = {}
        self._loaded_files: dict[str, FormattedFiles] = {}

        # ── 1. Build a unified list of tasks to load ──────────────────────────
        load_tasks = []
        is_single_input = inputs is not None

        if is_single_input:
            if isinstance(tree_name, list):
                raise ValueError(
                    "tree_name as a list is only supported with 'datasets'. "
                    "Pass a single string when using 'inputs'."
                )
            load_tasks.append(
                {
                    "name": "inputs",
                    "files": inputs,
                    "tree": tree_name.lstrip("/") if tree_name else tree_name,
                    "step": step_size,
                    "split_rg": split_row_groups,
                    "schema": None,
                    "base_dir": None,
                    "metadata": {},
                }
            )
        else:
            filesets = getattr(self.CONFIG, "filesets", None) or {}
            if not filesets:
                raise ValueError("No input path provided and no 'filesets' section in config.")

            # Normalize datasets
            if not datasets:
                datasets = list(filesets.keys())
            elif isinstance(datasets, str):
                datasets = [datasets]
            elif not (isinstance(datasets, list) and all(isinstance(d, str) for d in datasets)):
                raise ValueError(
                    f"Invalid 'datasets' value: {datasets!r}. Must be a string or list of strings."
                )

            invalid = [name for name in datasets if name not in filesets]
            if invalid:
                raise KeyError(
                    f"Dataset(s) {invalid} not found in config filesets. Available: {
                        list(filesets.keys())
                    }"
                )

            if isinstance(tree_name, list) and len(tree_name) != len(datasets):
                raise ValueError(
                    f"tree_name list length ({len(tree_name)}) must match datasets "
                    f"length ({len(datasets)})."
                )

            tree_names = tree_name if isinstance(tree_name, list) else [tree_name] * len(datasets)
            base_dir = os.path.dirname(self.CONFIG.path)

            for ds_name, ds_tree in zip(datasets, tree_names):
                ds_cfg = filesets[ds_name]
                if not ds_cfg.get("files"):
                    raise ValueError(f"Dataset '{ds_name}' has no 'files' key.")

                load_tasks.append(
                    {
                        "name": ds_name,
                        "files": ds_cfg["files"],
                        "tree": ds_tree if ds_tree is not None else ds_cfg.get("treename"),
                        "step": step_size if step_size is not None else ds_cfg.get("step_size"),
                        "split_rg": split_row_groups
                        if split_row_groups is not None
                        else ds_cfg.get("split_row_groups"),
                        "schema": ds_cfg.get("schema"),
                        "base_dir": base_dir,
                        "metadata": ds_cfg.get("metadata", {}),
                    }
                )

        # ── 2. Execute loading ───────────────────────────────
        for task in load_tasks:
            name = task["name"]
            self._loaded_files[name] = _format_input(
                task["files"],
                tree_path=task["tree"],
                max_files=maxfiles,
                base_dir=task["base_dir"],
                in_format=in_format,
            )
            self.events[name] = self._load_events(
                self._loaded_files[name],
                step_size=task["step"],
                split_row_groups=task["split_rg"],
                schema=task["schema"],
                name=name,
                in_format=in_format,
            )
            if not is_single_input:
                self.metadata[name] = task["metadata"]

        # ── 3. Post-process and print info ────────────────────────────────────
        if is_single_input:
            self.events = self.events[
                "inputs"
            ]  # Flatten dict to single dak.Array for explicit inputs

        if verbose:
            self.info()

    # ------------------------------------------------------------------
    # Internal loading
    # ------------------------------------------------------------------

    def _load_events(
        self,
        files: FormattedFiles,
        step_size: int | None,
        split_row_groups: bool | None,
        schema: object = None,
        name: str = "inputs",
        in_format: Literal["root", "parquet"] = "root",
    ) -> dak.Array:
        """Format files, load (ROOT or Parquet), apply pre-steps → ``dak.Array``."""
        if in_format == "parquet":
            # For Parquet, we ignore tree names so only keys (file paths) are relevant.
            events = dak.from_parquet(list(files.keys()), split_row_groups=split_row_groups)
        else:
            events = _load_from_root(
                files,
                self.CONFIG,
                step_size=step_size,
                schema_section=schema,
            )

        pre_steps = getattr(self.CONFIG, "pre-steps", None) or {}

        if pre_steps:
            return execute_pipeline(events, pre_steps, dataset=name if name != "inputs" else None)

        return events

    def info(self, max_files: int = 3, indent: int = -1) -> None:
        """
        Print a human-readable summary of the loaded NTuple.
        Shows loaded datasets, file listing, and partition counts.
        """

        # ── filesets mode ─────────────────────────────────────────────
        if isinstance(self.events, dict):
            filesets = getattr(self.CONFIG, "filesets", None) or {}
            color_msg(
                f"Loaded — {len(self.events)} dataset(s)",
                "cyan",
                bold=True,
                indentLevel=indent,
            )

            for ds_name, evts in self.events.items():
                loaded = self._loaded_files.get(ds_name, {})
                ds_cfg = filesets.get(ds_name, {})
                npart = getattr(evts, "npartitions", "?")

                _print_dataset_block(ds_name, loaded, npart, ds_cfg, max_files, indent)

        # ── inputs mode ───────────────────────────────────────────────
        else:
            loaded = self._loaded_files.get("inputs", {})
            npart = getattr(self.events, "npartitions", "?")

            _print_dataset_block("inputs", loaded, npart, max_files=max_files, indent=indent)
