"""
YAMLSchema — coffea BaseSchema subclass for ROOT ntuple files.

**Structural (schema) pass only.**  No actual array data is read here; the
schema operates exclusively on ``base_form`` (the Awkward *description* of
the data).

What it does
------------
Reads the ``Schema`` mapping from the YAML config.  For each collection entry
(dict-valued key) it calls :func:`~coffea.nanoevents.schemas.base.zip_forms`
to group the individual branch forms into a single **jagged record** with
``__record__ = "Particle"`` so Awkward dispatches to
:class:`~ydana.base.particle.ParticleRecord`.

Top-level string-valued entries become flat event-level scalar fields.
Numeric-valued entries are *constants* — injected after the factory returns
the dask array (see :func:`_inject_constants` in ``ntuple.py``).

Config injection
----------------
Because ``NanoEventsFactory`` calls ``schemaclass(base_form)`` with no extra
arguments, config injection is done via :meth:`YAMLSchema.with_config`:

.. code-block:: python

    schema_cls = YAMLSchema.with_config(schema_map)        # schema_map = config["Schema"] dict
    factory = NanoEventsFactory.from_root(files, schemaclass=schema_cls, ...)
"""

from __future__ import annotations

from warnings import warn

import awkward as ak
import dask_awkward as dak
from coffea.nanoevents.schemas.base import BaseSchema, zip_forms

# ---------------------------------------------------------------------------
# Module-level helpers (also imported by ntuple.py)
# ---------------------------------------------------------------------------


def _branches_from_schema(schema_map: dict) -> set[str]:
    """Return every ROOT branch name referenced in *schema_map*.

    Called by ``ntuple.py`` before creating the schema class so the branch
    allow-list can be passed to ``NanoEventsFactory.from_root``.
    """
    branches: set[str] = set()
    for key, val in schema_map.items():
        if isinstance(val, str):
            branches.add(val)  # event-level scalar alias
        elif isinstance(val, dict):
            for attr, v in val.items():
                if isinstance(v, str):
                    branches.add(v)  # particle-level attribute
    return branches


def _inject_constants(events: ak.Array | dak.Array, schema_map: dict) -> ak.Array | dak.Array:
    """Inject numeric constant fields declared in *schema_map* into *events*.

    Called after ``NanoEventsFactory.from_root(...).events()`` returns the
    dask array.  Numeric values have no ROOT branch behind them — they are
    added as virtual constant columns directly into the dask graph.
    """

    for key, val in schema_map.items():
        if isinstance(val, (int, float)):
            events = ak.with_field(events, val, key)  # event-level constant
        elif isinstance(val, dict):
            col = events[key]
            for attr, v in val.items():
                if isinstance(v, (int, float)):
                    col = ak.with_field(col, v, attr)  # particle-level constant
            events = ak.with_field(events, col, key)
    return events


# ---------------------------------------------------------------------------
# Schema class
# ---------------------------------------------------------------------------


class YAMLSchema(BaseSchema):
    """Groups flat ntuple branches into nested Awkward particle records.

    Parameters
    ----------
    base_form : dict
        The Awkward form dict produced by uproot/coffea (already pre-filtered
        to the allow-listed branches before this class is instantiated).

    Notes
    -----
    The ``schema_map`` (the value of the ``Schema:`` key in YAML) is bound at
    the class level via :meth:`with_config` before this constructor is called
    by coffea.
    """

    __dask_capable__ = True

    # Bound by with_config(); None means "not yet configured"
    _schema_map: dict | None = None

    # ------------------------------------------------------------------
    # Constructor — high-level orchestration only
    # ------------------------------------------------------------------

    def __init__(self, base_form: dict, version: str = "latest") -> None:
        schema_map = self.__class__._schema_map
        if schema_map is None:
            raise RuntimeError(
                "YAMLSchema has no schema_map. "
                "Use YAMLSchema.with_config(schema_map) before passing to NanoEventsFactory."
            )

        branch_forms = dict(zip(base_form.get("fields", []), base_form.get("contents", [])))
        consumed: set[str] = set()

        collection_forms = self._build_collections(schema_map, branch_forms, consumed)
        scalar_forms = self._build_scalars(schema_map, branch_forms, consumed)
        remaining_forms = {k: v for k, v in branch_forms.items() if k not in consumed}

        self._form = self._assemble_form(collection_forms, scalar_forms, remaining_forms, base_form)

    # ------------------------------------------------------------------
    # Private helpers — each does one thing
    # ------------------------------------------------------------------

    @staticmethod
    def _build_collection(
        key: str,
        attr_map: dict,
        branch_forms: dict,
        consumed: set[str],
    ) -> dict | None:
        """Try to build a zip_forms collection for one dict-valued schema entry.

        Branches are only marked as consumed if ``zip_forms`` succeeds.
        Returns the collection form dict, or ``None`` if nothing could be built.
        """
        member_forms: dict[str, dict] = {}
        local_consumed: set[str] = set()

        for attr_name, v in attr_map.items():
            if attr_name == "name":
                continue  # metadata key — not a branch
            if not isinstance(v, str):
                continue  # numeric constant — no branch form exists
            branch = v
            if branch not in branch_forms:
                warn(
                    f"Branch '{branch}' for collection '{key}.{attr_name}' "
                    "not found in schema; skipping."
                )
                continue
            member_forms[attr_name] = branch_forms[branch]
            local_consumed.add(branch)

        if not member_forms:
            return None  # all attributes are constants, nothing to zip

        try:
            form = zip_forms(member_forms, key, record_name="Particle")
            consumed |= local_consumed  # commit only on success
            # Embed the collection name so ParticleRecord can display it.
            display_name = attr_map.get("name", key)

            inner = form.get("content", form)  # ListOffsetArray → content; RecordArray → self
            inner.setdefault("parameters", {})["__record__"] = "Particle"
            inner.setdefault("parameters", {})["__collection__"] = display_name

            return form

        except (NotImplementedError, ValueError, KeyError) as exc:
            warn(f"zip_forms failed for collection '{key}': {exc}. Leaving branches flat.")
            return None

    @staticmethod
    def _build_collections(
        schema_map: dict,
        branch_forms: dict,
        consumed: set[str],
    ) -> dict[str, dict]:
        """Build one collection form per dict-valued entry in *schema_map*."""
        forms: dict[str, dict] = {}
        for key, val in schema_map.items():
            if not isinstance(val, dict):
                # Not a collection entry — skip.  Scalar aliases (string-valued) are handled
                # separately in _build_scalars.
                continue
            form = YAMLSchema._build_collection(key, val, branch_forms, consumed)
            if form is not None:
                forms[key] = form
        return forms

    @staticmethod
    def _build_scalars(
        schema_map: dict,
        branch_forms: dict,
        consumed: set[str],
    ) -> dict[str, dict]:
        """Return ``{alias: form}`` for every string-valued top-level entry."""
        forms: dict[str, dict] = {}
        for key, val in schema_map.items():
            if not isinstance(val, str):
                # Not a scalar alias entry — skip.  numeric constants (int/float-valued) are not
                # expected to have branch forms and are handled separately in _inject_constants.
                continue
            branch = val
            if branch not in branch_forms:
                warn(f"Event-level branch '{branch}' (key '{key}') not found in schema; skipping.")
                continue
            forms[key] = branch_forms[branch]
            consumed.add(branch)
        return forms

    @staticmethod
    def _assemble_form(
        collection_forms: dict,
        scalar_forms: dict,
        remaining_forms: dict,
        base_form: dict,
    ) -> dict:
        """Combine all partial forms into the final top-level Event RecordArray."""
        all_fields = list(collection_forms) + list(scalar_forms) + list(remaining_forms)
        all_contents = (
            list(collection_forms.values())
            + list(scalar_forms.values())
            + list(remaining_forms.values())
        )

        params: dict = dict(base_form.get("parameters", {}))
        if params.get("metadata") is None:
            params.pop("metadata", None)
        params.setdefault("metadata", {})
        params["__record__"] = "Event"

        return {
            "class": "RecordArray",
            "fields": all_fields,
            "contents": all_contents,
            "parameters": params,
            "form_key": None,
        }

    # ------------------------------------------------------------------
    # Config injection helper
    # ------------------------------------------------------------------

    @classmethod
    def with_config(cls, schema_map: dict) -> type:
        """Return a subclass of *YAMLSchema* pre-bound to *schema_map*.

        *schema_map* is the value of the ``Schema:`` key in the YAML config
        (a plain dict — not the whole config object).

        .. code-block:: python

            schema_cls = YAMLSchema.with_config(config["Schema"])
            events = NanoEventsFactory.from_root(..., schemaclass=schema_cls)
        """

        class _BoundSchema(cls):  # type: ignore[valid-type]
            _schema_map = schema_map

            def __init__(self, base_form: dict, version: str = "latest") -> None:
                super().__init__(base_form, version)

        _BoundSchema.__name__ = cls.__name__
        _BoundSchema.__qualname__ = f"{cls.__qualname__}[bound]"
        return _BoundSchema

    # ------------------------------------------------------------------
    # Behavior — combine Event + Particle behaviors
    # ------------------------------------------------------------------

    @classmethod
    def behavior(cls) -> dict:
        """Return the combined Event + Particle behavior dict."""
        from .event import behavior  # avoids circular import at module load time

        return behavior
