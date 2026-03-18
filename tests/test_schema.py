"""
Unit tests for ydana.base.schema.YAMLSchema.

Pure form-manipulation tests — no ROOT file, no awkward arrays.
Run with:  pytest tests/test_schema.py -v
"""

import pytest

from ydana.base.schema import YAMLSchema

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _numpy_form(branch: str) -> dict:
    """Minimal NumpyArray form that mirrors what uproot produces for a scalar branch."""
    return {
        "class": "NumpyArray",
        "primitive": "int32",
        "form_key": branch,
        "parameters": {},
    }


SCHEMA_MAP = {
    "run": "event_runNumber",  # scalar alias
    "label": 42,  # event-level constant (no branch)
    "digis": {
        "wh": "digi_wheel",
        "w": "digi_wire",
        "time": "digi_time",
        "dummy": -999,  # particle-level constant (no branch)
    },
    "genmuons": {
        "pt": "gen_pt",
        "pdgId": "gen_pdgId",
    },
    "ghost": {
        "x": "nonexistent_branch",  # all branches absent → collection skipped
    },
}

BASE_FORM = {
    "class": "RecordArray",
    "fields": [
        "digi_wheel",
        "digi_wire",
        "digi_time",
        "gen_pt",
        "gen_pdgId",
        "event_runNumber",
        "unrelated_branch",
    ],
    "contents": [
        _numpy_form("digi_wheel"),
        _numpy_form("digi_wire"),
        _numpy_form("digi_time"),
        _numpy_form("gen_pt"),
        _numpy_form("gen_pdgId"),
        _numpy_form("event_runNumber"),
        _numpy_form("unrelated_branch"),
    ],
    "parameters": {"metadata": None},
    "form_key": None,
}


@pytest.fixture(scope="module")
def form() -> dict:
    """Build the YAMLSchema form once and share it across all tests."""
    schema_cls = YAMLSchema.with_config(SCHEMA_MAP)
    schema = schema_cls(BASE_FORM)
    return schema._form


@pytest.fixture(scope="module")
def contents_by_field(form) -> dict:
    return dict(zip(form["fields"], form["contents"]))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYAMLSchemaForm:
    def test_top_level_record_name(self, form):
        assert form["parameters"]["__record__"] == "Event"

    def test_collections_present(self, form):
        assert "digis" in form["fields"]
        assert "genmuons" in form["fields"]

    def test_scalar_alias_present(self, form):
        """The alias key ('run') must appear, not the raw branch name."""
        assert "run" in form["fields"]
        assert "event_runNumber" not in form["fields"]

    def test_constant_not_in_form(self, form):
        """Numeric constants are injected post-factory; they must not appear here."""
        assert "label" not in form["fields"]

    def test_ghost_collection_absent(self, form):
        """A collection whose every branch is missing must be silently dropped."""
        assert "ghost" not in form["fields"]

    def test_unrelated_branch_passthrough(self, form):
        """Branches not claimed by any schema entry must appear in remaining_forms."""
        assert "unrelated_branch" in form["fields"]

    def test_scalar_form_key_preserved(self, contents_by_field):
        """The scalar alias must still point at the original ROOT branch name."""
        assert contents_by_field["run"]["form_key"] == "event_runNumber"

    def test_metadata_key_present(self, form):
        assert "metadata" in form["parameters"]


class TestYAMLSchemaContract:
    def test_raises_without_config(self):
        """Calling YAMLSchema directly (no with_config) must raise RuntimeError."""
        with pytest.raises(RuntimeError, match="with_config"):
            YAMLSchema(BASE_FORM)

    def test_with_config_returns_subclass(self):
        cls = YAMLSchema.with_config(SCHEMA_MAP)
        assert issubclass(cls, YAMLSchema)

    def test_bound_schema_name_unchanged(self):
        """The dynamic subclass keeps the parent's __name__ for readable tracebacks."""
        cls = YAMLSchema.with_config(SCHEMA_MAP)
        assert cls.__name__ == "YAMLSchema"


class TestCollectionParams:
    def _inner(self, contents_by_field: dict, key: str) -> dict:
        """Navigate ListOffsetArray → content (the inner RecordArray)."""
        form = contents_by_field[key]
        return form.get("content", form)

    def test_collection_name_defaults_to_key(self, contents_by_field):
        """When no 'name' key is in the YAML, __collection__ falls back to the YAML key."""
        inner = self._inner(contents_by_field, "digis")
        assert inner["parameters"]["__collection__"] == "digis"

    def test_custom_collection_name(self):
        """When 'name' is present in the YAML collection map, __collection__ uses it."""
        schema_map = {
            "digis": {
                "name": "DT Digis",
                "wh": "digi_wheel",
            }
        }
        base = {
            "class": "RecordArray",
            "fields": ["digi_wheel"],
            "contents": [_numpy_form("digi_wheel")],
            "parameters": {},
            "form_key": None,
        }
        form = YAMLSchema.with_config(schema_map)(base)._form
        contents = dict(zip(form["fields"], form["contents"]))
        inner = contents["digis"].get("content", contents["digis"])
        assert inner["parameters"]["__collection__"] == "DT Digis"

    def test_name_key_not_treated_as_branch(self):
        """'name' in the collection map must not trigger a missing-branch warning."""
        import warnings

        schema_map = {
            "digis": {
                "name": "DT Digis",
                "wh": "digi_wheel",
            }
        }
        base = {
            "class": "RecordArray",
            "fields": ["digi_wheel"],
            "contents": [_numpy_form("digi_wheel")],
            "parameters": {},
            "form_key": None,
        }
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning becomes an error
            YAMLSchema.with_config(schema_map)(base)
