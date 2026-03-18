from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import awkward as ak
import dask_awkward as dak
import pytest

from ydana.base.config import RUN_CONFIG, Config, set_run_config
from ydana.base.ntuple import NTuple

TREE = "dtNtupleProducer/DTTREE"


def _build_min_config(filesets: dict) -> Config:
    cfg = Config.__new__(Config)
    cfg.__dict__["path"] = str(Path.cwd())
    cfg.__dict__["filesets"] = filesets
    cfg.__dict__["pre-steps"] = {}
    return cfg


def _fake_events(npartitions: int = 1) -> dak.Array:
    return dak.from_awkward(ak.Array([{"x": 1}, {"x": 2}]), npartitions=npartitions)


def test_ntuple_single_input_flattens_to_single_array() -> None:
    cfg = _build_min_config({})
    fake_events = _fake_events(1)

    with (
        patch("ydana.base.ntuple._format_input", return_value={"dummy.root": TREE}) as mock_format,
        patch("ydana.base.ntuple._load_from_root", return_value=fake_events) as mock_load,
    ):
        ntuple = NTuple(
            inputs="dummy.root",
            tree_name=TREE,
            in_format="root",
            CONFIG=cfg,
            verbose=False,
        )

    assert ntuple.events is fake_events
    assert ntuple._loaded_files["inputs"] == {"dummy.root": TREE}
    mock_format.assert_called_once()
    mock_load.assert_called_once_with(
        {"dummy.root": TREE},
        cfg,
        step_size=None,
        schema_section=None,
    )


def test_ntuple_datasets_mode_returns_events_map_and_metadata() -> None:
    cfg = _build_min_config(
        {
            "DY": {"files": ["dy.root"], "treename": TREE, "metadata": {"year": 2024}},
            "Zprime": {"files": ["zp.root"], "treename": TREE, "metadata": {"year": 2023}},
        }
    )

    with (
        patch(
            "ydana.base.ntuple._format_input",
            side_effect=[{"dy.root": TREE}, {"zp.root": TREE}],
        ),
        patch.object(
            NTuple,
            "_load_events",
            side_effect=[_fake_events(2), _fake_events(1)],
        ) as mock_load_events,
    ):
        ntuple = NTuple(
            datasets=["DY", "Zprime"],
            in_format="root",
            CONFIG=cfg,
            verbose=False,
        )

    assert isinstance(ntuple.events, dict)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}
    assert ntuple.events["DY"].npartitions == 2
    assert ntuple.events["Zprime"].npartitions == 1
    assert ntuple.metadata["DY"]["year"] == 2024
    assert ntuple.metadata["Zprime"]["year"] == 2023
    assert mock_load_events.call_args_list[0].kwargs["name"] == "DY"
    assert mock_load_events.call_args_list[1].kwargs["name"] == "Zprime"


def test_load_events_parquet_uses_dak_loader_and_pipeline() -> None:
    cfg = _build_min_config({})
    cfg.__dict__["pre-steps"] = {"dummy": {"type": "selector", "expr": "events['x'] > 0"}}
    ntuple = NTuple.__new__(NTuple)
    ntuple.CONFIG = cfg
    raw_events = _fake_events(1)
    processed_events = Mock()

    with (
        patch("ydana.base.ntuple.dak.from_parquet", return_value=raw_events) as mock_from_parquet,
        patch("ydana.base.ntuple.execute_pipeline", return_value=processed_events) as mock_pipeline,
    ):
        result = ntuple._load_events(
            {"sample.parquet": None},
            step_size=None,
            split_row_groups=True,
            name="DY",
            in_format="parquet",
        )

    assert result is processed_events
    mock_from_parquet.assert_called_once_with(["sample.parquet"], split_row_groups=True)
    mock_pipeline.assert_called_once_with(raw_events, cfg.__dict__["pre-steps"], dataset="DY")


def test_load_events_root_returns_loader_output_without_pipeline() -> None:
    cfg = _build_min_config({})
    ntuple = NTuple.__new__(NTuple)
    ntuple.CONFIG = cfg
    raw_events = _fake_events(1)

    with patch("ydana.base.ntuple._load_from_root", return_value=raw_events) as mock_load_root:
        result = ntuple._load_events(
            {"sample.root": TREE},
            step_size=10,
            split_row_groups=None,
            schema={"fields": {}},
            name="inputs",
            in_format="root",
        )

    assert result is raw_events
    mock_load_root.assert_called_once_with(
        {"sample.root": TREE},
        cfg,
        step_size=10,
        schema_section={"fields": {}},
    )


def test_ntuple_requires_exactly_one_format() -> None:
    with pytest.raises(ValueError, match="in_format must be either"):
        NTuple(inputs="dummy.root", tree_name=TREE, in_format="invalid", verbose=False)


def test_ntuple_inputs_and_datasets_are_mutually_exclusive() -> None:
    cfg = _build_min_config({"DY": {"files": ["dummy.root"], "treename": TREE}})

    with pytest.raises(ValueError, match="mutually exclusive"):
        NTuple(
            inputs="dummy.root",
            datasets=["DY"],
            tree_name=TREE,
            in_format="root",
            CONFIG=cfg,
            verbose=False,
        )


def test_ntuple_requires_initialized_run_config() -> None:
    RUN_CONFIG.reset()

    with pytest.raises(RuntimeError, match="RUN_CONFIG is not initialized"):
        NTuple(in_format="root", maxfiles=1, verbose=False)


def test_ntuple_uses_initialized_run_config() -> None:
    cfg = _build_min_config(
        {
            "DY": {"files": ["dy.root"], "treename": TREE},
            "Zprime": {"files": ["zp.root"], "treename": TREE},
            "simulation": {"files": ["sim.root"], "treename": TREE},
        }
    )

    with (
        patch("ydana.base.ntuple._format_input", return_value={"dummy.root": TREE}),
        patch.object(NTuple, "_load_events", return_value=_fake_events(1)),
    ):
        try:
            set_run_config(cfg)
            ntuple = NTuple(in_format="root", maxfiles=1, verbose=False)
        finally:
            RUN_CONFIG.reset()

    assert isinstance(ntuple.events, dict)
    assert {"simulation", "DY", "Zprime"}.issubset(ntuple.events.keys())
