"""Example histogram definitions using the columnar ydana histogram API.

Each entry in ``histos`` is an instance of the :class:`Histogram` wrapper from
:mod:`ydana.base.histos`.

The ``func`` callable maps the input events array to a dictionary of 1D arrays
that correspond to the defined axes.

**Important:** Depending on the execution mode, ``events`` will be either a
materialized ``awkward.Array`` (eager / per-partition modes) or a completely
lazy ``dask_awkward.Array`` (in-memory mode). Therefore, the slicing and math
inside ``func`` must be array-agnostic (standard ``ak.*`` functions handle both).

**Note on Jagged Arrays:**
If extracting variables from jagged/nested collections (e.g., all muons per event
rather than just the leading one), you must explicitly flatten them into 1D arrays
(e.g., using ``ak.flatten``) before returning them.

**Note on Efficiencies:**
Histograms defined with a ``hist.axis.Boolean`` (like ``Muon_pt20_eff`` below)
are automatically recognized by the framework's ROOT serialization. When saved,
they are split dynamically into ``<name>_num`` (True bins) and ``<name>_den``
(Total bins) without user intervention.

These histograms assume:
  - ``events["genmuons"]`` exists with fields ``pt``, ``eta``, ``phi``.
  - ``events["dR"]`` exists (added by the ``add_genmuon_dR`` preprocessor).
  - Events have already been filtered to those with >= 2 gen-muons (via
    the ``select-has-genmuons`` pre-step), so ``[:, 0]`` and ``[:, 1]``
    are always valid.
"""

import awkward as ak
import hist

from ydana.base.histos import Histogram

# ---------------------------------------------------------------------------
# Histogram list
# ---------------------------------------------------------------------------

histos = [
    # --- Leading muon properties
    Histogram(
        hist.axis.Regular(20, 0, 1000, name="pt", label=r"Leading muon $p_T$ [GeV]"),
        name="LeadingMuon_pt",
        func=lambda events: {"pt": events["genmuons"]["pt"][:, 0]},
    ),
    Histogram(
        hist.axis.Regular(10, -3, 3, name="eta", label=r"Leading muon $\eta$"),
        name="LeadingMuon_eta",
        func=lambda events: {"eta": events["genmuons"]["eta"][:, 0]},
    ),
    # --- Subleading muon properties
    # (safe after select-has-genmuons pre-step guarantees >=2 gen-muons)
    Histogram(
        hist.axis.Regular(20, 0, 1000, name="pt", label=r"Subleading muon $p_T$ [GeV]"),
        name="SubLeadingMuon_pt",
        func=lambda events: {"pt": events["genmuons"]["pt"][:, 1]},
    ),
    Histogram(
        hist.axis.Regular(10, -3, 3, name="eta", label=r"Subleading muon $\eta$"),
        name="SubLeadingMuon_eta",
        func=lambda events: {"eta": events["genmuons"]["eta"][:, 1]},
    ),
    # --- Efficiency example: pT > 20 GeV
    # Shows explicit flattening of a jagged array and boolean axis usage!
    Histogram(
        hist.axis.Regular(200, 0, 1000, name="pt", label=r"muon $p_T$ [GeV]"),
        hist.axis.Boolean(name="pt20", label=r"muon $p_T > 20$ GeV"),
        name="Muon_pt20_eff",
        func=lambda events: {
            "pt": ak.flatten(events["genmuons"]["pt"]),
            "pt20": ak.flatten(events["genmuons"]["pt"] > 20),
        },
    ),
]
