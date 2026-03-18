from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

from ydana.analysis.histos_filler import fill_histos


def test_fill_histos_returns_early_when_no_histograms() -> None:
    with (
        patch("ydana.analysis.histos_filler.NTuple") as mock_ntuple,
        patch("ydana.analysis.histos_filler._histos.from_config", return_value=[]),
        patch("ydana.analysis.histos_filler._histos.fill") as mock_fill,
    ):
        mock_ntuple.return_value.events = Mock()
        fill_histos(inputs=["dummy.root"], in_format="root", verbose=False)

    mock_fill.assert_not_called()


def test_fill_histos_dispatches_single_dataset_with_tag() -> None:
    fake_events = Mock()
    fake_histos = [SimpleNamespace(name="h1")]

    with (
        patch("ydana.analysis.histos_filler.NTuple") as mock_ntuple,
        patch("ydana.analysis.histos_filler._histos.from_config", return_value=fake_histos),
        patch("ydana.analysis.histos_filler._histos.fill") as mock_fill,
    ):
        mock_ntuple.return_value.events = fake_events

        fill_histos(
            inputs=["dummy.root"],
            in_format="root",
            tag="_unit",
            outfolder="/tmp/out",
            ncores=1,
            verbose=False,
        )

    mock_fill.assert_called_once()
    _, kwargs = mock_fill.call_args
    assert kwargs["tag"] == "_inputs_unit"
    assert kwargs["label"] == "inputs"


def test_fill_histos_dispatches_all_dataset_entries() -> None:
    fake_histos = [SimpleNamespace(name="h1")]

    with (
        patch("ydana.analysis.histos_filler.NTuple") as mock_ntuple,
        patch("ydana.analysis.histos_filler._histos.from_config", return_value=fake_histos),
        patch("ydana.analysis.histos_filler._histos.fill") as mock_fill,
    ):
        mock_ntuple.return_value.events = {"DY": Mock(), "Zprime": Mock()}

        fill_histos(
            datasets=["DY", "Zprime"],
            in_format="root",
            tag="_v1",
            outfolder="/tmp/out",
            ncores=1,
            verbose=False,
        )

    assert mock_fill.call_count == 2
    called_tags = {call.kwargs["tag"] for call in mock_fill.call_args_list}
    assert called_tags == {"_DY_v1", "_Zprime_v1"}
