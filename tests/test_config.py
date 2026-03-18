from pathlib import Path

import pytest

from ydana.base.config import RUN_CONFIG, Config, get_run_config, set_run_config


def test_config_loader():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    cfg = Config(str(fixtures_dir / "include_main.yaml"))

    assert cfg.simple_key == "simple"
    assert cfg.included_map == {"alpha": 1, "beta": 2}
    assert cfg.merged_map == {"alpha": 1, "beta": 2, "gamma": 3, "delta": 4}
    assert cfg.included_list == ["x", "y", "z"]


def test_get_run_config_raises_when_uninitialized() -> None:
    RUN_CONFIG.reset()

    with pytest.raises(RuntimeError, match="RUN_CONFIG is not initialized"):
        get_run_config()


def test_set_run_config_accepts_config_instance() -> None:
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    cfg = Config(str(fixtures_dir / "include_main.yaml"))

    try:
        set_run_config(cfg)
        active = get_run_config()
    finally:
        RUN_CONFIG.reset()

    assert active is cfg
    assert active.simple_key == "simple"
