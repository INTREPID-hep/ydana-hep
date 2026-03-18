"""Columnar selectors — functions with signature
``fn(events: ak.Array) -> ak.Array``.

Event-level selectors return a boolean ``ak.Array`` of length ``nevents``.
Particle-level selectors (used with a ``target:`` in the pipeline) return
a variable-length boolean array matching the collection shape.
"""

from __future__ import annotations

import awkward as ak


def has_genmuons(events: ak.Array) -> ak.Array:
    """Keep only events that contain at least one gen-muon."""
    return ak.num(events["genmuons"]) > 0


def filter_genmuons_pdgid(events: ak.Array) -> ak.Array:
    """Particle-level mask: keep only muons/anti-muons (|pdgId| == 13)."""
    return abs(events["genmuons"]["pdgId"]) == 13
