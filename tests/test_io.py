from __future__ import annotations

from pathlib import Path

import awkward as ak
import dask_awkward as dak
import uproot

from ydana.base.io import _flatten_awkward_to_dict, dump_to_parquet, dump_to_root


def _events() -> ak.Array:
    return ak.Array(
        [
            {"run": 1, "digis": [{"wh": -1}, {"wh": 2}]},
            {"run": 2, "digis": [{"wh": 1}]},
        ]
    )


def test_flatten_awkward_to_dict_collects_scalar_and_collection_fields() -> None:
    branches = _flatten_awkward_to_dict(_events())

    assert "event_run" in branches
    assert "digis_wh" in branches
    assert branches["event_run"].to_list() == [1, 2]
    assert branches["digis_wh"].to_list() == [[-1, 2], [1]]


def test_dump_to_parquet_eager_writes_single_file(tmp_path: Path) -> None:
    outdir = tmp_path / "parquet-eager"

    dump_to_parquet(_events(), str(outdir), ncores=1, verbose=False)

    outpath = outdir / "parquets" / "dumpedEvents.parquet"
    assert outpath.exists()

    loaded = ak.from_parquet(outpath)
    assert loaded["run"].to_list() == [1, 2]


def test_dump_to_parquet_lazy_writes_dataset_directory(tmp_path: Path) -> None:
    outdir = tmp_path / "parquet-lazy"
    lazy_events = dak.from_awkward(_events(), npartitions=2)

    dump_to_parquet(lazy_events, str(outdir), ncores=1, verbose=False)

    parts = list((outdir / "parquets").glob("*.parquet"))
    assert parts, "Expected partition parquet files in output directory"


def test_dump_to_root_eager_writes_root_file(tmp_path: Path) -> None:
    outdir = tmp_path / "root-eager"

    dump_to_root(_events(), str(outdir), tag="_unit", ncores=1, verbose=False)

    root_path = outdir / "roots" / "dumpedEvents_unit.root"
    assert root_path.exists()
    with uproot.open(root_path) as fin:
        assert "Events/tree" in fin


def test_dump_to_root_lazy_per_partition_writes_multiple_files(tmp_path: Path) -> None:
    outdir = tmp_path / "root-lazy"
    lazy_events = dak.from_awkward(_events(), npartitions=2)

    dump_to_root(lazy_events, str(outdir), tag="_pp", ncores=1, verbose=False)

    part_files = sorted((outdir / "roots").glob("dumpedEvents_pp_*.root"))
    assert len(part_files) == 2
