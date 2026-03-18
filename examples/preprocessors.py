"""Columnar preprocessors — functions with signature
``fn(events: ak.Array) -> None``.

Each function receives the full event array and **mutates it in-place** via
``events["field"] = value`` (leverages ``ak.Array.__setitem__`` /
``dak.Array.__setitem__``).  No return value is used.
For nested collections: ``events["col"] = ak.with_field(events["col"], value, "key")``.
"""

from __future__ import annotations

import awkward as ak
import numpy as np


def add_genmuon_dR(events: ak.Array) -> None:
    """Add a per-event ``dR`` field between the first two gen-muons.

    Events with fewer than two gen-muons get ``dR = -999.0``.
    Mutates ``events`` in-place by setting ``events["dR"]``.
    """
    genmuons = events["genmuons"]
    has_two = ak.num(genmuons) >= 2

    eta = ak.pad_none(genmuons["eta"], 2, axis=1)
    phi = ak.pad_none(genmuons["phi"], 2, axis=1)

    eta0 = ak.fill_none(eta[:, 0], 0.0)
    eta1 = ak.fill_none(eta[:, 1], 0.0)
    phi0 = ak.fill_none(phi[:, 0], 0.0)
    phi1 = ak.fill_none(phi[:, 1], 0.0)

    d_eta = eta1 - eta0
    d_phi = phi1 - phi0
    d_phi = ak.where(d_phi > np.pi, d_phi - 2.0 * np.pi, d_phi)
    d_phi = ak.where(d_phi < -np.pi, d_phi + 2.0 * np.pi, d_phi)

    dR = np.sqrt(d_eta**2 + d_phi**2)
    events["dR"] = ak.where(has_two, dR, -999.0)
