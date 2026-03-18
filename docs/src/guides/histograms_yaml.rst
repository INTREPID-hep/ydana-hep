histograms guide
================

Histogram definitions live in Python, while YAML controls which ones are
loaded and filled at runtime. This guide starts from the
:class:`ydana.base.histos.Histogram` API, then shows how ``histograms.yaml``
selects histograms for a given run.

.. contents::
   :local:
   :depth: 1


Define Histograms in Python
---------------------------

The core object is :class:`ydana.base.histos.Histogram`.
Each instance wraps a ``hist`` object plus a ``func(events)`` callback that
returns the columns to fill.

Minimal contract:

* ``name`` identifies the histogram in config and output.
* Axis ``name=`` values must match keys returned by ``func``.
* ``func`` receives the YDANA-HEP ``events`` object (lazy ``dak.Array`` or eager
  ``ak.Array`` depending on execution mode).

.. literalinclude:: ../../../examples/histograms.py
   :language: python
   :lines: 42-77
   :caption: examples/histograms.py (excerpt)

.. important::

    **Key rules:**

    * The ``name`` kwarg should be unique across loaded sources.
    * ``func`` must return a ``dict`` whose keys match axis names.
    * For jagged/nested collections, call ``ak.flatten()`` when needed.


Select Histograms from YAML
---------------------------

YDANA-HEP discovers which histograms to fill through
:func:`ydana.base.histos.from_config`.

It supports **both** of the following configuration layouts:

1. Nested map under ``histograms``:

.. code-block:: yaml

   histograms:
     histo_sources:
       - histograms
     histo_names:
       - LeadingMuon_pt
       - LeadingMuon_eta

2. Top-level keys:

.. code-block:: yaml

   histo_sources:
     - histograms

   histo_names:
     - LeadingMuon_pt
     - LeadingMuon_eta

If ``histo_names`` is provided, only matching names are loaded. If it is
omitted or empty, all histograms found in the listed sources are used.

``histo_sources`` entries are importable Python module names, and each module
must expose a ``histos`` list of :class:`ydana.base.histos.Histogram` objects.


Histogram Selection Example
---------------------------

``histograms.yaml`` selects *which* histograms to fill:

.. literalinclude:: ../../../examples/yamls/histograms.yaml
   :language: yaml
   :lines: 1-17
   :caption: examples/yamls/histograms.yaml

``histo_sources`` modules must be on ``sys.path`` (for example, ``examples/``
when running from there).


Efficiency Histograms
---------------------

Histograms with a ``hist.axis.Boolean`` axis are handled specially:
the framework splits them into ``<name>_num`` (True bins) and
``<name>_den`` (all bins) when writing to ROOT. No extra user code is
needed.


Parametrised Histograms with ``expand``
----------------------------------------

Use :func:`ydana.base.histos.expand` to produce multiple histograms from
a single template — useful for parametrised histograms.

.. code-block:: python

   from ydana.base.histos import expand

   histos += expand(
       Histogram(
           hist.axis.Regular(20, 0, 1000, name="pt", label=r"$p_T$ [GeV]"),
           name="Muon_pt_{station}",
           func=lambda events, station=None: {
               "pt": ak.flatten(
                   events["genmuons"]["pt"]
                   [events["segments"]["st"] == station]
               ),
           },
       ),
       station=[1, 2, 3, 4],
   )


Lazy vs Eager Fill
------------------

The fill path depends on the array type passed to
:meth:`ydana.base.histos.Histogram.fill`:

* **``dak.Array``** (default ``fill-histos``) — builds a lazy Dask task
  graph; no data is read until ``dask.compute()`` is called.
* **``ak.Array``** (per-partition workers) — synchronous fill; the
  histogram is accumulated in memory and immediately written to disk.

The ``--per-partition`` flag to ``fill-histos`` selects the per-partition
path automatically.


See Also
--------

* :class:`ydana.base.histos.Histogram` — full API reference.
* :func:`ydana.base.histos.expand` — parametrised histogram factory.
* :func:`ydana.base.histos.fill` — module-level fill entry point.

