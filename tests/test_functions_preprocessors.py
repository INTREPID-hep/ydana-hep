from __future__ import annotations

import awkward as ak

from ydana.utils.functions import reconstruct_nested_ids


def _events() -> ak.Array:
    return ak.Array(
        [
            {
                "tps": [{"pt": 1.0}, {"pt": 2.0}],
                "tps_matched_showers_ids": [10, 11],
                "tps_matched_showers_ids_n": [1, 1],
            },
            {
                "tps": [{"pt": 3.0}],
                "tps_matched_showers_ids": [20, 21],
                "tps_matched_showers_ids_n": [2],
            },
            {
                "tps": [{"pt": 4.0}, {"pt": 5.0}],
                "tps_matched_showers_ids": [30],
                "tps_matched_showers_ids_n": [0, 1],
            },
        ]
    )


def test_reconstruct_nested_ids_default_out_field() -> None:
    events = _events()

    pp = reconstruct_nested_ids(
        "tps_matched_showers_ids",
        "tps_matched_showers_ids_n",
        "tps",
    )
    pp(events)

    # Default output name strips trailing "_ids".
    assert "tps_matched_showers" in ak.fields(events["tps"])
    assert events["tps"]["tps_matched_showers"].to_list() == [
        [[10], [11]],
        [[20, 21]],
        [[], [30]],
    ]


def test_reconstruct_nested_ids_custom_out_field() -> None:
    events = _events()

    pp = reconstruct_nested_ids(
        "tps_matched_showers_ids",
        "tps_matched_showers_ids_n",
        "tps",
        out_field="matched_showers_ids",
    )
    pp(events)

    assert "matched_showers_ids" in ak.fields(events["tps"])
    assert events["tps"]["matched_showers_ids"].to_list() == [
        [[10], [11]],
        [[20, 21]],
        [[], [30]],
    ]
