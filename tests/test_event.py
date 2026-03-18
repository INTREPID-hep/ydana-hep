"""
Unit tests for ydana.base.event.EventRecord.

Pure awkward-Array construction — no ROOT file needed.
Run with:  pytest tests/test_event.py -v
"""

import re

import awkward as ak
import pytest

from ydana.base.event import EventRecord, behavior
from ydana.utils.functions import find_field_by_pattern

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def events():
    """Two-event mock array with one scalar, two collections (one empty)."""
    return ak.with_name(
        ak.Array(
            [
                {
                    "num": 42,
                    "digis": [{"wh": 1, "time": 100}, {"wh": 2, "time": 200}],
                    "genmuons": [{"pt": 25.0, "pdgId": 13}],
                },
                {
                    "num": 43,
                    "digis": [{"wh": 3, "time": 300}],
                    "genmuons": [],  # empty collection
                },
            ]
        ),
        name="Event",
        behavior=behavior,
    )


@pytest.fixture(scope="module")
def events_no_id():
    """Event array with no id-like field — forces layout.at fallback."""
    return ak.with_name(
        ak.Array([{"wh": 1}, {"wh": 2}]),
        name="Event",
        behavior=behavior,
    )


def _strip_ansi(s: str) -> str:
    return re.sub(r"\033\[[^m]*m", "", s)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEventRecordDispatch:
    def test_dispatch_to_event_record(self, events):
        assert isinstance(events[0], EventRecord)
        assert isinstance(events[1], EventRecord)

    def test_behavior_keys_present(self):
        """behavior dict must expose Event, Particle and *Particle so both levels dispatch."""
        assert "Event" in behavior
        assert "Particle" in behavior
        assert "*Particle" in behavior

    def test_fields(self, events):
        assert set(events[0].fields) == {"num", "digis", "genmuons"}


class TestFindIdField:
    """Tests for the _ID_PATTERN + find_field_by_pattern used by EventRecord.id."""

    def test_matches_num(self):
        assert find_field_by_pattern(["num", "digis"], EventRecord._ID_PATTERN) == "num"

    def test_matches_compound_name(self):
        assert (
            find_field_by_pattern(["event_number", "digis"], EventRecord._ID_PATTERN)
            == "event_number"
        )

    def test_no_match_returns_none(self):
        assert find_field_by_pattern(["wh", "time"], EventRecord._ID_PATTERN) is None

    def test_case_insensitive(self):
        """_ID_PATTERN uses re.IGNORECASE — uppercase variants must match."""
        pat = EventRecord._ID_PATTERN
        assert find_field_by_pattern(["INDEX", "digis"], pat) == "INDEX"
        assert find_field_by_pattern(["Event_ID"], pat) == "Event_ID"
        assert find_field_by_pattern(["NUMBER"], pat) == "NUMBER"
        assert find_field_by_pattern(["Idx"], pat) == "Idx"
        assert find_field_by_pattern(["ev"], pat) == "ev"
        assert find_field_by_pattern(["evnum"], pat) == "evnum"

    def test_first_match_wins(self):
        """When multiple candidates exist, the first field in order is returned."""
        assert find_field_by_pattern(["num", "event_id"], EventRecord._ID_PATTERN) == "num"


class TestEventId:
    def test_id_uses_id_field_value(self, events):
        """_id returns the raw field value, not a string."""
        assert events[0].id == 42
        assert events[1].id == 43

    def test_id_fallback_to_positional_index(self, events_no_id):
        assert events_no_id[0].id == 0
        assert events_no_id[1].id == 1

    def test_id_is_cached(self, events):
        """cached_property must return the same object on repeated access."""
        ev = events[0]
        assert ev.id is ev.id


class TestRepresentation:
    def test_repr_contains_label(self, events):
        assert repr(events[0]) == "<Event 42>"
        assert repr(events[1]) == "<Event 43>"

    def test_repr_fallback_index(self, events_no_id):
        assert repr(events_no_id[1]) == "<Event 1>"

    def test_str_contains_label(self, events):
        plain = _strip_ansi(str(events[0]))
        assert "42" in plain

    def test_show_contains_all_field_names(self, events, capsys):
        """Verbose field listing has moved to show()."""
        events[0].show()
        plain = _strip_ansi(capsys.readouterr().out)
        for field in events[0].fields:
            assert field in plain

    def test_show_empty_collection_shown(self, events, capsys):
        """Empty genmuons must still appear as a collection line, not be silently dropped."""
        events[1].show()
        plain = _strip_ansi(capsys.readouterr().out)
        assert "genmuons" in plain
        assert "0 items" in plain


class TestCollectionDetection:
    def test_scalar_not_detected_as_collection(self, events):
        val = events[0]["num"]
        assert not (hasattr(val, "fields") and len(val.fields) > 0)

    def test_non_empty_collection_detected(self, events):
        val = events[0]["digis"]
        assert hasattr(val, "fields") and len(val.fields) > 0

    def test_empty_collection_still_detected(self, events):
        """An empty collection still carries .fields — must not fall through to scalar path."""
        val = events[1]["genmuons"]
        assert hasattr(val, "fields") and len(val.fields) > 0
