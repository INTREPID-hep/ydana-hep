"""
Awkward Array behavior classes for particle records and collections.

- ``ParticleRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Particle"]``
- ``behavior``        → dict to be passed to :func:`coffea.nanoevents.NanoEventsFactory.from_root`

The class and behavior key names are intentionally generic so this package is
reusable across different ntuple schemas and analysis domains.
"""

import re
from collections.abc import Iterable
from functools import cached_property
from typing import Any

import awkward as ak

from ..utils.functions import color_msg, find_field_by_pattern


class ParticleRecord(ak.Record):
    """Single particle record (one row of a jagged particle collection).

    Dispatched automatically by Awkward when the inner ``__record__`` parameter
    of the collection is ``"Particle"``.

    Equivalent to the old per-event ``Particle`` instance but backed by a lazy
    Awkward slice rather than in-memory Python attributes.
    """

    _IDX_PATTERN = re.compile(r"\b(idx|id|index|num(ber)?)\b", re.IGNORECASE)

    @cached_property
    def id(self) -> Any:
        """Numeric identifier for this particle within its collection.

        Computed once and cached.  Scans the particle fields for one whose
        name matches a common index pattern (``idx``, ``id``, ``index``,
        ``num``, …) and returns its value directly.  Falls back to the
        positional index within the parent array (``layout.at``) when no
        such field is present.

        The raw value is returned — not a string.
        """
        idx_field = find_field_by_pattern(self.fields, self._IDX_PATTERN)
        if idx_field is not None:
            return self[idx_field]
        return self.layout.at

    @staticmethod
    def ids_from_array(array: ak.Array) -> ak.Array:
        """Return the ``id`` column for an array of ``ParticleRecord`` elements.

        Array-level equivalent of :attr:`id`, used when operating on a full
        (possibly doubly-jagged) array rather than a single record:

        * If any field in the array matches :attr:`_IDX_PATTERN`, return that
          column (``array[id_field]``).
        * Otherwise fall back to ``ak.local_index(array, axis=-1)``, which
          produces the positional index of each element — identical to
          ``layout.at`` used by :attr:`id` on a single record.
        """
        id_field = find_field_by_pattern(list(ak.fields(array)), ParticleRecord._IDX_PATTERN)
        if id_field is not None:
            return array[id_field]
        return ak.local_index(array, axis=-1)

    # def __repr__(self) -> str:
    #     collection = self.layout.parameter("__collection__") or "Particle"
    #     parts = ", ".join(f"{f}={self[f]!r}" for f in self.fields)
    #     return f"<{collection}[{self.id}] {parts}>"

    def __repr__(self) -> str:
        if isinstance(self, ak.Record):
            # For a single particle, just show the name and ID (e.g., <Muon[0]>)
            collection = self.layout.parameter("__collection__") or "Particle"
            return f"<{collection}[{self.id}]>"

        # For the array itself, Awkward's native engine handle the preview
        return super().__repr__()

    def show(
        self,
        indentLevel: int = 0,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
        **kwargs: object,
    ) -> str:
        """Human-readable summary"""
        fields = [
            f
            for f in self.fields
            if (include is None or f in include) and (exclude is None or f not in exclude)
        ]
        collection = self.layout.parameter("__collection__") or "Particle"
        header = color_msg(
            f"{collection}[{self.id}] -->",
            color=kwargs.pop("color", "yellow"),
            indentLevel=indentLevel,
            return_str=True,
            **kwargs,
        )
        body = color_msg(
            ", ".join(f"{f.capitalize()}: {self[f]}" for f in fields),
            indentLevel=indentLevel + 1,
            return_str=True,
        )
        print("\n".join([header, body]))


class ParticleArray(ak.Array):
    """Collection of particle records.

    Dispatched automatically by Awkward for columnar collection access
    (``events["digis"]``).  Single-event slices (``events[0]["digis"]``)
    return a plain ``ak.Array`` — use columnar access for best results.
    """

    @property
    def id(self) -> ak.Array:
        """Vectorized access to particle IDs.

        This allows calling 'events.particles.id' on an array of particles,
        consistent with the '.id' property on a single ParticleRecord.
        """
        # Call the static method already defined in ParticleRecord
        return ParticleRecord.ids_from_array(self)

    def __repr__(self) -> str:
        return f"<ParticleArray len={len(self)}>"


# ---------------------------------------------------------------------------
# Behavior dictionary — the single source of truth for Awkward dispatch.
# Merge this dict (or import it) into the combined behavior dict passed to
# NanoEventsFactory.
# ---------------------------------------------------------------------------
behavior: dict = {
    "Particle": ParticleRecord,
    "*Particle": ParticleArray,
}
