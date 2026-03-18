from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import ydana.cli as cli


def _config_file(tmp_path: Path) -> Path:
    path = tmp_path / "run_config.yaml"
    path.write_text("{}\n")
    return path


def test_ydana_cli_entrypoint_help(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["ydana", "--help"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "fill-histos" in captured.out
    assert "dump-events" in captured.out


@pytest.mark.parametrize(
    "subcommand",
    ["fill-histos", "dump-events", "merge-histos", "merge-roots"],
)
def test_ydana_subcommand_help(
    subcommand: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["ydana", subcommand, "--help"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 0
    assert subcommand in capsys.readouterr().out


def test_ydana_main_dispatches_dump_events_with_explicit_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_file = _config_file(tmp_path)
    received: dict[str, object] = {}

    def fake_func(
        inputs: list[str] | None = None,
        tree_name: str | None = None,
        in_format: str = "root",
        out_format: str = "parquet",
        maxfiles: int = -1,
        outfolder: str = "",
    ) -> None:
        received.update(
            {
                "inputs": inputs,
                "tree_name": tree_name,
                "in_format": in_format,
                "out_format": out_format,
                "maxfiles": maxfiles,
                "outfolder": outfolder,
            }
        )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ydana",
            "dump-events",
            "-cf",
            str(config_file),
            "-i",
            "dummy.root",
            "-tr",
            "Events/tree",
            "--in-format",
            "root",
            "--out-format",
            "parquet",
            "--maxfiles",
            "1",
            "-o",
            str(tmp_path / "cli-out"),
        ],
    )

    with (
        patch("ydana.cli.get_callable_from_src", return_value=fake_func),
        patch("ydana.cli.set_run_config") as mock_set_run_config,
        patch(
            "ydana.cli.get_run_config",
            return_value=SimpleNamespace(path=str(config_file)),
        ),
        patch("ydana.cli.ensure_on_syspaths") as mock_ensure_paths,
    ):
        cli.main()

    mock_set_run_config.assert_called_once_with(os.path.abspath(config_file))
    mock_ensure_paths.assert_called_once()
    assert received["inputs"] == ["dummy.root"]
    assert received["tree_name"] == "Events/tree"
    assert received["in_format"] == "root"
    assert received["out_format"] == "parquet"
    assert received["maxfiles"] == 1
    assert received["outfolder"] == str(tmp_path / "cli-out")


def test_ydana_main_dispatches_fill_histos_with_explicit_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_file = _config_file(tmp_path)
    received: dict[str, object] = {}

    def fake_func(
        inputs: list[str] | None = None,
        tree_name: str | None = None,
        in_format: str = "root",
        verbose: bool = True,
    ) -> None:
        received.update(
            {
                "inputs": inputs,
                "tree_name": tree_name,
                "in_format": in_format,
                "verbose": verbose,
            }
        )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ydana",
            "fill-histos",
            "-cf",
            str(config_file),
            "-i",
            "dummy.root",
            "-tr",
            "Events/tree",
            "--in-format",
            "root",
            "--maxfiles",
            "1",
            "-o",
            str(tmp_path / "fill-out"),
            "--no-verbose",
        ],
    )

    with (
        patch("ydana.cli.get_callable_from_src", return_value=fake_func),
        patch("ydana.cli.set_run_config"),
        patch(
            "ydana.cli.get_run_config",
            return_value=SimpleNamespace(path=str(config_file)),
        ),
        patch("ydana.cli.ensure_on_syspaths"),
    ):
        cli.main()

    assert received["inputs"] == ["dummy.root"]
    assert received["tree_name"] == "Events/tree"
    assert received["in_format"] == "root"
    assert received["verbose"] is False


def test_ydana_main_requires_config_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["ydana", "fill-histos", "-i", "dummy.root"])

    with patch("ydana.cli.get_callable_from_src", return_value=lambda **kwargs: None):
        with pytest.raises(FileNotFoundError, match="No configuration file provided"):
            cli.main()
