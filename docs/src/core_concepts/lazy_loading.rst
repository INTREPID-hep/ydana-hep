Lazy Loading
============

.. contents::
   :local:
   :depth: 1

When you call :class:`ydana.base.ntuple.NTuple`, three things happen — all without touching disk:

**Schema resolution.**

   The ``Schema:`` section of your YAML config is parsed by
   :class:`ydana.base.schema.YAMLSchema`, a
   :class:`~coffea.nanoevents.schemas.base.BaseSchema` subclass.
   For each dict-valued key it calls
   :func:`~coffea.nanoevents.schemas.base.zip_forms` to group flat ROOT branch
   forms into a nested Awkward *record* (``__record__ = "Particle"``),
   dispatched to :class:`ydana.base.particle.ParticleRecord`. String-valued
   top-level keys map a single branch to an event-level scalar alias.
   Numeric-valued entries are *constant* fields that require no branch read and
   are injected after the factory returns
   (see :func:`ydana.base.schema.inject_constants`).

   Because ``NanoEventsFactory`` calls ``schemaclass(base_form)`` with no extra
   arguments, the trick to have a "custom" schema map is bound to the class beforehand via
   :meth:`YAMLSchema.with_config <ydana.base.schema.YAMLSchema.with_config>`:

   .. code-block:: python

      schema_cls = YAMLSchema.with_config(config.Schema)
      events = NanoEventsFactory.from_root(files, schemaclass=schema_cls, mode="dask")

   Standard Coffea schemas (e.g. ``NanoAODSchema``) can also be used by passing 
   their class name as a plain string to ``Schema:`` in the YAML config. If
   ``Schema`` is omitted, ``BaseSchema`` is used as a fallback (all branches loaded flat).

**Lazy array construction.**

   ``NanoEventsFactory`` runs in ``dask`` mode, wrapping every field in a
   ``dask_awkward.Array`` — a *task graph* node. No data is read; only the
   Awkward *form* (type description) is inspected to build the graph.
   The result is a structured nested array:
   ``events.genmuons.pt`` is a ``dak.Array`` of shape
   ``[nevents, nmuons]`` where both dimensions are still unknown at this stage.

**Pre-steps registration.**

   Before returning, :class:`~ydana.base.ntuple.NTuple` runs
   :func:`ydana.base.pipeline.execute_pipeline` on the events to apply any
   ``pre-steps`` declared in the config. These transforms extend or filter
   the task graph. See :doc:`lazy_execution`.


Single vs Multi-Dataset
-----------------------

When a single explicit path is given, ``ntuple.events`` is a plain
``dak.Array``:

.. code-block:: python

   ntuple = NTuple("/path/to/data/")
   bx = ntuple.events["digis"]["BX"]  # dak.Array — no I/O yet
   print(bx.compute())               # triggers read

When named datasets are used (via ``filesets.yaml``), ``ntuple.events`` is a ``dict[str, dak.Array]``:

.. code-block:: python

   ntuple = NTuple(datasets=["DY", "Zprime"]) # filesets defines at least DY and Zprime datasets
   dy_bx     = ntuple.events["DY"]["digis"]["BX"]
   zprime_bx = ntuple.events["Zprime"]["digis"]["BX"]

   import dask
   dy_result, zprime_result = dask.compute(dy_bx, zprime_bx)
   # Both datasets are read in a single optimised scheduler call.


Supported Input Formats
-----------------------

``NTuple`` accepts the same local input forms as :func:`uproot.dask`, including
single paths, glob patterns, directories, explicit file lists, and the
uproot-native ``{filepath: treepath}`` dict. The extended dict form that
specifies explicit entry ranges (``steps``) is also supported:

.. list-table::
   :header-rows: 1
   :widths: 48 52

   * - Input
     - Meaning
   * - ``"file.root"``
     - Single ROOT file
   * - ``"/dir/*.root"``
     - Glob pattern — all matching files
   * - ``"/dir/"``
     - Directory — auto-detects ``.root`` or ``.parquet``
   * - ``["a.root", "b.root"]``
     - Explicit file list
   * - ``{"a.root": "treename"}``
     - Dict (uproot-native, one tree per file)
   * - ``{"f.root": {"object_path": "tree", "steps": [[0, 10000], ...]}}``
     - Dict with explicit entry ranges (uproot step form)
   * - ``"output.parquet"``
     - Single Parquet file
   * - ``["a.parquet", "b.parquet"]``
     - Parquet file list

.. warning::

   Remote protocols — ``xroot://``, ``https://``, and CMS DAS queries — are
   **not yet supported**. YDANA-HEP's file resolver currently handles only
   local paths and glob patterns. Use local copies or pre-staged files.


Partitioning
------------

Set ``step_size`` as argument or via ``filesets.yaml`` (with ``step``) to split large files into chunks.
Each chunk becomes one Dask partition, enabling parallel and per-partition
execution modes.

.. code-block:: yaml

   DY:
     treename: "dtNtupleProducer/DTTREE"
     step: 50000   # entries per partition
     files:
       - "../ntuples/DY/*.root"


See Also
--------

* :doc:`lazy_execution` — what happens when ``.compute()`` is called.
* :class:`ydana.base.ntuple.NTuple` — full API reference.

