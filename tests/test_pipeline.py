"""Unit tests for ydana.base.pipeline."""

import awkward as ak
import pytest

from ydana.base.pipeline import execute_pipeline, topological_sort


def test_topological_sort_parallel_level():
    steps = {
        "a": {"type": "selector", "expr": "events['x'] > 0"},
        "b": {"type": "preprocessor", "expr": "events['x2'] = events['x'] * 2", "needs": ["a"]},
        "c": {"type": "preprocessor", "expr": "events['x3'] = events['x'] * 3", "needs": ["a"]},
    }
    levels = topological_sort(steps)
    assert [[name for name, _ in grp] for grp in levels] == [["a"], ["b", "c"]]


def test_topological_sort_cycle_raises():
    steps = {
        "a": {"type": "selector", "expr": "events['x'] > 0", "needs": ["b"]},
        "b": {"type": "selector", "expr": "events['x'] > 0", "needs": ["a"]},
    }
    with pytest.raises(ValueError, match="[Cc]ycle"):
        topological_sort(steps)


def test_topological_sort_unknown_dep_raises():
    steps = {
        "a": {"type": "preprocessor", "expr": "events", "needs": ["nonexistent"]},
    }
    with pytest.raises(ValueError, match="unknown"):
        topological_sort(steps)


def test_execute_pipeline_selector_and_preprocessor():
    events = ak.Array(
        [
            {"num": 1, "x": 10, "digis": [{"wh": -1}, {"wh": 2}]},
            {"num": 2, "x": 20, "digis": [{"wh": 1}, {"wh": -2}]},
            {"num": 3, "x": 30, "digis": [{"wh": 3}]},
        ]
    )
    steps = {
        "keep_even": {"type": "selector", "expr": "events['num'] % 2 == 0"},
        "add_x2": {
            "type": "preprocessor",
            "expr": "events['x2'] = events['x'] * 2",
            "needs": ["keep_even"],
        },
        "add_xp1": {
            "type": "preprocessor",
            "expr": "events['xp1'] = events['x'] + 1",
            "needs": ["keep_even"],
        },
        "keep_positive_wh": {
            "type": "selector",
            "target": "digis",
            "expr": "events['digis']['wh'] > 0",
            "needs": ["add_x2", "add_xp1"],
        },
    }
    out = execute_pipeline(events, steps)
    assert out["num"].to_list() == [2]
    assert out["x2"].to_list() == [40]
    assert out["xp1"].to_list() == [21]
    assert out["digis"]["wh"].to_list() == [[1]]


def test_selector_event_level():
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {"keep_gt1": {"type": "selector", "expr": "events['x'] > 1"}}
    out = execute_pipeline(events, steps)
    assert out["x"].to_list() == [2, 3]


def test_selector_particle_level():
    events = ak.Array(
        [
            {"x": 1, "hits": [{"v": -1}, {"v": 2}]},
            {"x": 2, "hits": [{"v": 3}, {"v": -4}]},
        ]
    )
    steps = {"keep_pos": {"type": "selector", "target": "hits", "expr": "events['hits']['v'] > 0"}}
    out = execute_pipeline(events, steps)
    assert out["hits"]["v"].to_list() == [[2], [3]]


def test_preprocessor_mutates_events():
    events = ak.Array([{"x": 1}, {"x": 2}])
    steps = {"double_x": {"type": "preprocessor", "expr": "events['x'] = events['x'] * 2"}}
    out = execute_pipeline(events, steps)
    assert out["x"].to_list() == [2, 4]


def test_unknown_step_type_raises():
    events = ak.Array([{"x": 1}])
    steps = {"bad": {"type": "transformer", "expr": "events"}}
    with pytest.raises(ValueError, match="Unknown step type"):
        execute_pipeline(events, steps)


def test_empty_steps_returns_events_unchanged():
    events = ak.Array([{"x": 1}, {"x": 2}])
    out = execute_pipeline(events, {})
    assert out["x"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# Tests for `on` key (dataset-scoped steps)
# ---------------------------------------------------------------------------


def test_on_str_step_skipped_for_other_dataset():
    """A step with `on: DY` must not run when dataset is 'Zprime'."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1", "on": "DY"},
    }
    out = execute_pipeline(events, steps, dataset="Zprime")
    # step skipped → all events survive
    assert out["x"].to_list() == [1, 2, 3]


def test_on_str_step_applied_for_matching_dataset():
    """A step with `on: DY` must run when dataset is 'DY'."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1", "on": "DY"},
    }
    out = execute_pipeline(events, steps, dataset="DY")
    assert out["x"].to_list() == [2, 3]


def test_on_list_step_applied_for_matching_dataset():
    """`on: [DY, Zprime]` applies to both."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1", "on": ["DY", "Zprime"]},
    }
    for ds in ("DY", "Zprime"):
        out = execute_pipeline(events, steps, dataset=ds)
        assert out["x"].to_list() == [2, 3], f"failed for dataset={ds!r}"


def test_on_list_step_skipped_for_other_dataset():
    """`on: [DY, Zprime]` does not apply to 'QCD'."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1", "on": ["DY", "Zprime"]},
    }
    out = execute_pipeline(events, steps, dataset="QCD")
    assert out["x"].to_list() == [1, 2, 3]


def test_on_absent_applies_to_all_datasets():
    """A step without `on` must apply regardless of dataset."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1"},
    }
    for ds in (None, "DY", "Zprime", "anything"):
        out = execute_pipeline(events, steps, dataset=ds)
        assert out["x"].to_list() == [2, 3], f"failed for dataset={ds!r}"


def test_on_filtered_needs_ref_dropped_gracefully():
    """When step A is filtered out by `on`, step B's `needs: [A]` must not raise."""
    events = ak.Array([{"x": 1}, {"x": 2}, {"x": 3}])
    steps = {
        # this step is only for DY — skipped for Zprime
        "keep_gt1": {"type": "selector", "expr": "events['x'] > 1", "on": "DY"},
        # this step needs keep_gt1, but keep_gt1 is inactive for Zprime
        "add_x2": {
            "type": "preprocessor",
            "expr": "events['x2'] = events['x'] * 2",
            "needs": ["keep_gt1"],
        },
    }
    # should not raise; add_x2 runs after keep_gt1 is dropped for Zprime
    out = execute_pipeline(events, steps, dataset="Zprime")
    assert out["x2"].to_list() == [2, 4, 6]
