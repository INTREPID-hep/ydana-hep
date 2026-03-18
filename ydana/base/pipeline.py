"""
Pipeline executor for YAML-driven columnar analysis workflows.

Topological ordering
--------------------
Steps are grouped by dependency depth (level 0 = no dependencies, level 1 =
depends on at least one level-0 step, etc.).  Within each level, **selectors
run before preprocessors** to discard unwanted events/particles as early as
possible before any expensive computation.

Step semantics
--------------
* ``type: selector``, no ``target`` — event-level filter:
  ``events = events[fn(events)]``   where ``fn`` returns ``bool[nevents]``

* ``type: selector``, with ``target: "tps"`` — particle-level filter:
  ``mask = fn(events)``  (bool per particle, variable-length)
  ``events = ak.with_field(events, events["tps"][mask], "tps")``

* ``type: preprocessor`` — arbitrary transform:
    ``fn(events)``  where ``fn`` **mutates** the event array directly using
    ``events["field"] = value`` (leverages ``ak.Array.__setitem__`` /
    ``dak.Array.__setitem__``).  No return value is used.  For nested
    collections use ``events["col"] = ak.with_field(events["col"], …, "key")``.

Step definition keys
--------------------
The ``pre-steps:`` key in YAML is a **mapping** (dict), where each key is the
unique step name.  No ``name:`` sub-key is needed.

* ``type``       — ``"selector"`` or ``"preprocessor"``
* ``target``     — (selector only) collection name for particle-level filter
* ``expr``       — inline Python evaluated with ``{"events": events, "ak": ak}``
* ``src``        — dotted import path to ``fn(events) -> None``  (mutates in-place)
* ``needs``      — list of step names that must run first (default ``[]``)
* ``on``         — dataset name (str) or list of names this step is restricted
  to.  Omit (or ``null``) to apply to every dataset / plain inputs.

Exactly one of ``expr`` / ``src`` must be present.
"""

from __future__ import annotations

from collections.abc import Callable

import awkward as ak
import dask_awkward as dak

from ..utils.functions import get_callable_from_src

EventArray = ak.Array | dak.Array

# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


def topological_sort(steps: dict[str, dict]) -> list[list[dict]]:
    """Group pipeline steps by dependency depth.

    Parameters
    ----------
    steps : dict[str, dict]
        Mapping from step name to step body as parsed from YAML ``pre-steps``.
        The dict key is the canonical step name.

    Returns
    -------
    list[list[tuple[str, dict]]]
        Dependency levels. Each level contains ``(name, step)`` pairs that can
        run in any order relative to each other.

    Raises
    ------
    ValueError
        Raised when a step references unknown dependencies, when ``needs`` is
        not a list, or when a cycle is detected in the graph.
    """
    if not steps:
        return []

    deps_map: dict[str, list[str]] = {}
    for name, step in steps.items():
        deps = step.get("needs", [])
        if not isinstance(deps, list):
            raise ValueError(f"Step '{name}': needs must be a list of step names.")
        unknown = set(deps) - steps.keys()
        if unknown:
            raise ValueError(f"Step '{name}' needs unknown step(s): {unknown}")
        deps_map[name] = deps

    level_cache: dict[str, int] = {}

    def _level(name: str) -> int:
        if name in level_cache:
            return level_cache[name]
        # Detect cycles via a sentinel
        level_cache[name] = -1  # "in progress"
        parents = deps_map[name]
        if any(level_cache.get(p) == -1 for p in parents):
            raise ValueError(f"Cycle detected in pipeline involving step '{name}'.")
        result = 0 if not parents else 1 + max(_level(p) for p in parents)
        level_cache[name] = result
        return result

    for name in steps:
        _level(name)

    max_level = max(level_cache.values(), default=0)
    return [
        [(name, steps[name]) for name in steps if level_cache[name] == lvl]
        for lvl in range(max_level + 1)
    ]


# ---------------------------------------------------------------------------
# Step function factory
# ---------------------------------------------------------------------------


def _make_fn(name: str, step: dict, kind: str = "eval") -> Callable[[EventArray], object]:
    """Return the callable described by a step's ``expr`` or ``src`` key.

    Parameters
    ----------
    kind : {'eval', 'exec'}
        ``'eval'`` — compiles ``expr`` as an expression; return value is used
        (selectors: returns a boolean mask).
        ``'exec'`` — compiles ``expr`` as a statement so that assignments such
        as ``events["field"] = value`` are valid; return value is discarded
        (preprocessors).
    """
    has_expr = "expr" in step
    has_src = "src" in step

    if has_expr and has_src:
        raise ValueError(f"Step '{name}': specify exactly one of 'expr' or 'src', not both.")
    if not has_expr and not has_src:
        raise ValueError(f"Step '{name}': must have exactly one of 'expr' or 'src'.")

    if has_expr:
        if kind == "exec":
            code = compile(step["expr"], f"<pre-steps:{name}>", "exec")

            def _fn(events: EventArray, _c: object = code) -> None:
                exec(_c, {"events": events, "ak": ak})

            return _fn
        code = compile(step["expr"], f"<pre-steps:{name}>", "eval")
        return lambda events, _c=code: eval(_c, {"events": events, "ak": ak})

    return get_callable_from_src(step["src"])


# ---------------------------------------------------------------------------
# Pipeline executor
# ---------------------------------------------------------------------------


def execute_pipeline(
    events: EventArray,
    steps: dict[str, dict],
    dataset: str | None = None,
) -> EventArray:
    """Execute pipeline steps in dependency order.

    Within each dependency level, **selectors run before preprocessors** to
    reduce the dataset size before any expensive computation.

    Selectors return a boolean mask; the executor applies it internally.
    Preprocessors **mutate** the event array in-place via ``__setitem__``
    (``events["field"] = value``); no return value is captured.

    All real parallelism is provided by dask at the file/partition level.
    Step ordering within a level is determined by the ``needs`` graph
    (topological sort); within the same level, selectors always precede
    preprocessors.

    Parameters
    ----------
    events : ak.Array or dask_awkward.Array
        The loaded event array.  If lazy (dask-awkward), no computation is
        triggered — this function only builds graph nodes.
    steps : dict[str, dict]
        Pipeline step definitions as parsed from the YAML ``pre-steps:`` key.
        Each key is the step name; each value is the step body (``type``,
        ``expr``/``src``, optional ``needs``, optional ``target``,
        optional ``on``).
    dataset : str or None, optional
        Name of the dataset currently being loaded (e.g. ``"DY"``).
        Steps whose ``on`` key does not match this name are skipped.
        ``None`` / absent ``on`` → step applies to every dataset.

    Returns
    -------
    ak.Array or dask_awkward.Array
        The transformed event array (still lazy if input was lazy).
    """
    # ── filter steps by `on` key ─────────────────────────────────────────
    active_steps: dict[str, dict] = {}
    for sname, step in steps.items():
        on = step.get("on")
        if on is None:
            active_steps[sname] = step
        elif isinstance(on, str):
            if on == dataset:
                active_steps[sname] = step
        elif isinstance(on, list):
            if dataset in on:
                active_steps[sname] = step

    # Drop `needs` refs to steps not active for this dataset so the
    # topological sort does not raise on unknown names.
    cleaned_steps: dict[str, dict] = {
        sname: {
            **step,
            "needs": [n for n in step.get("needs", []) if n in active_steps],
        }
        for sname, step in active_steps.items()
    }

    for level_group in topological_sort(cleaned_steps):
        # Fail fast: validate step types before executing anything in this level.
        unknown = [
            step["type"]
            for _, step in level_group
            if step["type"] not in {"selector", "preprocessor"}
        ]
        if unknown:
            raise ValueError(
                f"Unknown step type(s): {unknown}. Must be 'selector' or 'preprocessor'."
            )

        selectors = [(n, s) for n, s in level_group if s["type"] == "selector"]
        preprocessors = [(n, s) for n, s in level_group if s["type"] == "preprocessor"]

        # Selectors first: drop unwanted events/particles before any expensive work.
        for name, step in selectors:
            fn = _make_fn(name, step)
            target = step.get("target")
            mask = fn(events, **step.get("kwargs", {}))
            if target:
                # particle-level: mask within the named collection
                events = ak.with_field(events, events[target][mask], target)
            else:
                # event-level: drop entire events
                events = events[mask]

        # Preprocessors: mutate events in-place via __setitem__, applied sequentially.
        for name, step in preprocessors:
            _make_fn(name, step, kind="exec")(events, **step.get("kwargs", {}))

    return events


# ---------------------------------------------------------------------------
# Quick smoke-test
# Run with:  python -m ydana.base.pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import dask_awkward as dak

    events = ak.Array(
        [
            {"num": 1, "x": 10, "digis": [{"wh": -1}, {"wh": 2}]},
            {"num": 2, "x": 20, "digis": [{"wh": 1}, {"wh": -2}]},
            {"num": 3, "x": 30, "digis": [{"wh": 3}]},
        ]
    )

    steps = {
        "keep_even_events": {
            "type": "selector",
            "expr": "events['num'] % 2 == 0",
        },
        "add_x2": {
            "type": "preprocessor",
            "expr": "events['x2'] = events['x'] * 2",
            "needs": ["keep_even_events"],
        },
        "add_xp1": {
            "type": "preprocessor",
            "expr": "events['xp1'] = events['x'] + 1",
            "needs": ["keep_even_events"],
        },
        "keep_positive_wh": {
            "type": "selector",
            "target": "digis",
            "expr": "events['digis']['wh'] > 0",
            "needs": ["add_x2", "add_xp1"],
        },
    }

    print("=" * 60)
    print("pipeline.py smoke-test")
    print("=" * 60)

    levels = topological_sort(steps)
    print("Topological levels:", [[name for name, _ in grp] for grp in levels])
    print("Input num:", events["num"].to_list())
    print("Input digis.wh:", events["digis"]["wh"].to_list())

    out = execute_pipeline(events, steps)

    print("Output num:", out["num"].to_list())
    print("Output x2:", out["x2"].to_list())
    print("Output xp1:", out["xp1"].to_list())
    print("Output digis.wh:", out["digis"]["wh"].to_list())

    assert out["num"].to_list() == [2]
    assert out["x2"].to_list() == [40]
    assert out["xp1"].to_list() == [21]
    assert out["digis"]["wh"].to_list() == [[1]]

    print("\n--- Dask graph demo ---")
    devents = dak.from_awkward(events, npartitions=2)
    dout = execute_pipeline(devents, steps)

    graph = dout.__dask_graph__()
    print("Graph type:", type(graph).__name__)
    if hasattr(graph, "layers"):
        print("Graph layers:", list(graph.layers.keys()))
    if hasattr(graph, "dependencies"):
        print("\nLayer dependencies (trimmed):")
        for lname in graph.layers:
            if any(
                tok in lname for tok in ("multiply", "add-", "with-field", "from-awkward", "x-")
            ):
                deps = sorted(graph.dependencies.get(lname, []))
                print(f"  - {lname} <- {deps}")
    print("Approx task count:", len(graph))

    try:
        dout.visualize(filename="pipeline_dask_graph.svg")
        print("Graph visualization written to: pipeline_dask_graph.svg")
    except Exception as exc:
        print(f"Graph visualization skipped ({type(exc).__name__}: {exc})")

    dout_comp = dout.compute()
    assert dout_comp["num"].to_list() == [2]
    assert dout_comp["x2"].to_list() == [40]
    assert dout_comp["xp1"].to_list() == [21]
    assert dout_comp["digis"]["wh"].to_list() == [[1]]

    print("All assertions passed ✓")
