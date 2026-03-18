from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ydana.analysis.events_dumper import dump_events

TREE = "dtNtupleProducer/DTTREE"


def test_dump_events_requires_exactly_one_output_format(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="in_format must be either"):
        dump_events(inputs=[], outfolder=str(tmp_path), in_format="invalid")

    with pytest.raises(ValueError, match="out_format must be either"):
        dump_events(inputs=[], outfolder=str(tmp_path), out_format="invalid")


def test_dump_events_dispatches_single_input_to_root(tmp_path: Path) -> None:
    fake_events = Mock()
    fake_ntuple = Mock(events=fake_events)

    with (
        patch("ydana.analysis.events_dumper.NTuple", return_value=fake_ntuple) as mock_ntuple,
        patch("ydana.analysis.events_dumper.dump_to_root") as mock_dump_root,
        patch("ydana.analysis.events_dumper.dump_to_parquet") as mock_dump_parquet,
    ):
        dump_events(
            inputs=["dummy.root"],
            tree_name=TREE,
            maxfiles=1,
            in_format="root",
            out_format="root",
            ncores=1,
            outfolder=str(tmp_path),
            tag="_unit",
            verbose=False,
        )

    mock_ntuple.assert_called_once()
    mock_dump_parquet.assert_not_called()
    _, kwargs = mock_dump_root.call_args
    assert kwargs["events"] is fake_events
    assert kwargs["outfolder"] == str(tmp_path)
    assert kwargs["tag"] == "_unit"
    assert kwargs["label"] == "inputs"


def test_dump_events_dispatches_dataset_map_to_parquet(tmp_path: Path) -> None:
    fake_ntuple = Mock(events={"DY": Mock(), "Zprime": Mock()})

    with (
        patch("ydana.analysis.events_dumper.NTuple", return_value=fake_ntuple),
        patch("ydana.analysis.events_dumper.dump_to_root") as mock_dump_root,
        patch("ydana.analysis.events_dumper.dump_to_parquet") as mock_dump_parquet,
    ):
        dump_events(
            datasets=["DY", "Zprime"],
            in_format="root",
            out_format="parquet",
            ncores=1,
            outfolder=str(tmp_path),
            tag="_unit",
            verbose=False,
        )

    mock_dump_root.assert_not_called()
    assert mock_dump_parquet.call_count == 2
    called_outfolders = {call.kwargs["outfolder"] for call in mock_dump_parquet.call_args_list}
    assert called_outfolders == {str(tmp_path)}
