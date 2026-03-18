"""
Unit tests for ydana.base.particle.ParticleRecord and ParticleArray.

Pure awkward-Array construction — no ROOT file needed.
Run with:  pytest tests/test_particle.py -v
"""

import re

import awkward as ak
import pytest

from ydana.base.particle import ParticleArray, ParticleRecord, behavior

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def digis():
    """Jagged array: 2 events with variable-length particle collections."""
    return ak.with_name(
        ak.Array(
            [
                [{"wh": 1, "sl": 1, "time": 100}, {"wh": 1, "sl": 2, "time": 200}],
                [{"wh": 2, "sl": 1, "time": 300}],
            ]
        ),
        name="Particle",
        behavior=behavior,
    )


def _strip_ansi(s: str) -> str:
    return re.sub(r"\033\[[^m]*m", "", s)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParticleRecordDispatch:
    def test_leaf_dispatch_fires(self, digis):
        assert isinstance(digis[0][0], ParticleRecord)
        assert isinstance(digis[1][0], ParticleRecord)

    def test_behavior_keys_present(self):
        assert "Particle" in behavior
        assert "*Particle" in behavior

    def test_behavior_values(self):
        assert behavior["Particle"] is ParticleRecord
        assert behavior["*Particle"] is ParticleArray

    def test_fields(self, digis):
        assert set(digis[0][0].fields) == {"wh", "sl", "time"}


class TestParticleArrayDispatch:
    def test_outer_array_is_plain_ak_array(self, digis):
        """In awkward 2.x *Particle dispatch does not fire on plain jagged arrays
        — expected behavior, ParticleArray is reserved for coffea NanoEventsFactory output."""
        assert not isinstance(digis, ParticleArray)
        assert not isinstance(digis[0], ParticleArray)


class TestParticleRecordRepr:
    def test_repr_format(self, digis):
        r = repr(digis[0][0])
        assert r.startswith("<Particle[")
        assert r.endswith(">")

class TestParticleRecordStr:
    def test_show_include(self, digis, capsys):
        """Filtering is now in show(), not __str__."""
        digis[0][0].show(include=["wh", "time"])
        plain = _strip_ansi(capsys.readouterr().out)
        assert "Wh" in plain
        assert "Time" in plain
        assert "Sl" not in plain

    def test_show_exclude(self, digis, capsys):
        """Filtering is now in show(), not __str__."""
        digis[0][0].show(exclude=["sl"])
        plain = _strip_ansi(capsys.readouterr().out)
        assert "Wh" in plain
        assert "Time" in plain
        assert "Sl" not in plain


class TestToList:
    def test_returns_dict(self, digis):
        assert isinstance(digis[0][0].to_list(), dict)

    def test_correct_values(self, digis):
        assert digis[0][0].to_list() == {"wh": 1, "sl": 1, "time": 100}
        assert digis[1][0].to_list() == {"wh": 2, "sl": 1, "time": 300}

    def test_equality_via_to_list(self, digis):
        p0 = digis[0][0]
        p2 = digis[1][0]
        assert p0.to_list() == p0.to_list()
        assert p0.to_list() != p2.to_list()


class TestParticleId:
    def test_id_fallback_to_positional_index_when_no_idx_field(self, digis):
        """No idx-like field → _id falls back to layout.at (positional index)."""
        assert digis[0][0].id == 0

    def test_id_fallback_ignores_collection_name(self):
        """__collection__ parameter does not affect _id — only field values do."""
        records = ak.with_parameter(
            ak.with_name(ak.Array([{"wh": 1}]), name="Particle", behavior=behavior),
            "__collection__",
            "digis",
        )
        assert records[0].id == 0  # positional fallback

    def test_id_uses_idx_field_value(self):
        """When a field matches _IDX_PATTERN, _id returns that field's value."""
        raw = ak.with_name(
            ak.Array([[{"idx": 7, "wh": 1}]]),
            name="Particle",
            behavior=behavior,
        )
        assert raw[0][0].id == 7

    def test_repr_uses_id_in_brackets(self):
        """__repr__ formats as <Collection[id] ...>."""
        raw = ak.with_name(
            ak.Array([[{"number": 3, "wh": 2}]]),
            name="Particle",
            behavior=behavior,
        )
        assert repr(raw[0][0]).startswith("<Particle[3]")
