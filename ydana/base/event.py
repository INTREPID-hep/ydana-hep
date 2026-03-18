"""
Awkward Array behavior class for event records.

- ``EventRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Event"]``
- ``behavior``     → dict combining Event + Particle behaviors 
                     for :class:`~coffea.nanoevents.NanoEventsFactory`

The class and behavior key names are intentionally generic.
"""

import re
from functools import cached_property
from typing import Any

import awkward as ak

from ..utils.functions import color_msg, find_field_by_pattern
from .particle import behavior as particle_behavior


class EventRecord(ak.Record):
    """Single event record, backed by a lazy Awkward array slice.

    Dispatched automatically by Awkward when the top-level array has
    ``__record__ = "Event"`` in its parameters (set by ``YAMLSchema``).

    Equivalent to the old ``Event`` class but without per-event Python objects.
    """

    # ------------------------------------------------------------------
    # Representation helpers
    # ------------------------------------------------------------------

    _ID_PATTERN = re.compile(r"(ev(ent)?|num(ber)?|index|idx)", re.IGNORECASE)

    @cached_property
    def id(self) -> Any:
        """Numeric identifier for this event.

        Computed once and cached.  Scans the event fields for one whose name
        matches a common event-ID pattern (``event``, ``run``, ``number``,
        ``index``, ``idx``, …) and returns its value directly.  Falls back
        to the positional index within the parent array (``layout.at``) when
        no such field is present.

        The raw value (int, numpy scalar, …) is returned — not a string —
        so callers can compare, format, or log it as needed.
        """
        id_field = find_field_by_pattern(self.fields, self._ID_PATTERN)
        if id_field is not None:
            val = self[id_field]
            if not (hasattr(val, "fields") and val.fields):
                return val
        return self.layout.at

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return f"<Event {self.id}>"

    def show(self, indentLevel: int = 0) -> str:
        """Verbose summary of all fields in this event record.

        Visual convention
        -----------------
        - Yellow  : event header
        - Cyan    : particle collection label  (field has nested sub-fields)
        - Purple  : particle count
        - Green   : scalar field label
        - (none)  : scalar field value
        """
        lines = [
            color_msg(
                f"------ Event {self.id} ------",
                color="yellow",
                indentLevel=indentLevel,
                return_str=True,
            )
        ]
        for f in self.fields:
            val = self[f]
            is_collection = hasattr(val, "fields") and len(val.fields) > 0
            if is_collection:
                # Particle collection — cyan label + purple count
                lines.append(
                    color_msg(
                        f"{f}",
                        color="purple",
                        indentLevel=indentLevel + 1,
                        return_str=True,
                    )
                    + color_msg(
                        f"({len(val)} items, fields: {val.fields})",
                        color="none",
                        indentLevel=-1,
                        return_str=True,
                    )
                )
            else:
                # Scalar — green label, plain value
                lines.append(
                    color_msg(
                        f"{f}:",
                        color="green",
                        indentLevel=indentLevel + 1,
                        return_str=True,
                    )
                    + color_msg(f" {val}", color="none", indentLevel=-1, return_str=True)
                )
        print("\n".join(lines))


# ---------------------------------------------------------------------------
# Combined behavior dictionary used by NanoEventsFactory.
# Includes both Event and Particle behaviors so each level dispatches correctly.
# ---------------------------------------------------------------------------
behavior: dict = {
    "Event": EventRecord,
    **particle_behavior,
}
