YAML Basics
===========

YDANA-HEP entire behavior is encoded throug ``yaml`` files. It is possible to have a single top-level 
config file that composes smaller YAML files through ``!include`` directives.

.. contents::
   :local:
   :depth: 1


Main config
-----------

``run_config.yaml`` is the entry-point passed to every CLI command via ``-cf``:

.. code-block:: yaml

   # run_config.yaml
   filesets:   !include filesets.yaml     # ← named datasets
   Schema:     !include schema.yaml       # ← branch-to-field mapping
   pre-steps:  !include pre_steps.yaml    # ← ordered transforms
   histograms: !include histograms.yaml   # ← what to fill

The ``!include`` helps to keep each concern in its own file. You can also inline any
section directly if you prefer a single-file config.

.. note::

  YAML loading is managed by :class:`ydana.base.config.Config`, which resolves
  ``!include`` directives and stores top-level keys as attributes on the config
  object. At runtime, the active configuration is exposed through the singleton
  ``RUN_CONFIG`` (see :func:`ydana.base.config.get_run_config`).

  For programmatic usage, you can set it explicitly with
  :func:`ydana.base.config.set_run_config`, passing either a custom
  :class:`ydana.base.config.Config` instance or a config-file path.

  For deeper details on lifecycle and access patterns, see
  :doc:`../core_concepts/yaml_driven_workflow`.


Datasets — ``filesets.yaml``
----------------------------

Each top-level key is a *dataset name* used as an output tag and as a key in
the multi-dataset ``ntuple.events`` dict. Sintax try to follow standard coffea's fileset definition.
``xroot``, ``https``, and ``DAS`` datasets are not soported yet for the YDANA-HEP files resolver. 

.. code-block:: yaml

   DY:
     treename: "dtNtupleProducer/DTTREE"   # ROOT TTree path
     files:
       - "../ntuples/DY/DTDPGNtuple_*.root"  # glob patterns accepted
     metadata:
       label: "DY — Phase2 WShower simulation"
       version: "15_1_0_pre2"

   Zprime:
     treename: "dtNtupleProducer/DTTREE"
     files:
       - "../ntuples/Zprime/DTDPGNtuple_*.root"
     metadata:
       label: "Z' — Phase2 Concentrator simulation"
       version: "12_4_2"

``metadata`` is arbitrary user data attached to ``ntuple.metadata["DY"]``
at runtime — useful for labels and provenance tracking.


Mapping branches — ``schema.yaml``
------------------------------------
The ``Schema`` section tells YDANA-HEP how to group ROOT branches into named record
fields.

This configuration allows user to specifically indicated how to group each branch into collections, this is suported throug the customed YDANASchema class. This
design aims to mitigate possible scenerios where tradition coffea schemas don't load data as you wish, but,
you can always still indicated standard schemas passing ``Schema: [coffea schema name]``. If not provided,
``BaseSchema`` is tried by default.

Multiple schema by dataset are suported by specifiing in each fileset the ``schema`` filed. The follow is a simple example.

.. code-block:: yaml

   # Event-level scalars
   number: "event_eventNumber"   # key: "branch_name"

   # Particle collections — each attribute maps to one ROOT branch
   genmuons:
     pt:     "gen_pt"
     eta:    "gen_eta"
     phi:    "gen_phi"
     charge: "gen_charge"
     pdgId:  "gen_pdgId"

   tps:
     wh:      "ph2TpgPhiEmuAm_wheel"
     quality: "ph2TpgPhiEmuAm_quality"
     BX:      "ph2TpgPhiEmuAm_BX"
     # ...

After loading, ``event.genmuons.pt`` is a ``dask_awkward.Array`` of shape ``nevents * nmuons``.

.. tip::

   Fields can also be constants (integers) rather than branch names.


Defining transforms — ``pre_steps.yaml``
-----------------------------------------

Pre-steps are ordered transformations. YDANA-HEP resolves execution order
automatically from ``needs:`` declarations via
:func:`ydana.base.pipeline.topological_sort`.

.. code-block:: yaml

   # Level 0 — no dependencies
   compute-digi-BX:
     type: preprocessor
     expr: "events['digis'] = ak.with_field(events['digis'],
                                             events['digis']['time'] // 25, 'BX')"

   select-has-genmuons:
     type: selector
     expr: "ak.num(events['genmuons']) > 1"

   # Level 1 — only runs after select-has-genmuons
   compute-muon-DR:
     type: preprocessor
     src: "preprocessors.add_genmuon_dR"   # dotted import path
     needs: [select-has-genmuons]

Step semantics at a glance:

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - ``type``
     - ``target``
     - What happens
   * - selector
     - *(absent)*
     - ``events = events[fn(events)]``
   * - selector
     - ``"tps"``
     - Filter the ``tps`` collection per-event
   * - preprocessor
     - *(any)*
     - ``fn(events)`` mutates ``events`` in-place

See :doc:`../core_concepts/yaml_driven_workflow` for a full pipeline diagram.


Declaring histograms — ``histograms.yaml``
------------------------------------------

.. code-block:: yaml

   histo_sources:
     - histograms            # importable Python module defining a `histos` list

   histo_names:
     - LeadingMuon_pt        # must match Histogram(name=...) in the module
     - LeadingMuon_eta
     - muon_DR
     - Muon_pt20_eff

``histo_sources`` lists importable Python modules that each define a ``histos``
list of :class:`ydana.base.histos.Histogram` objects. The names in
``histo_names`` must exist in at least one of those modules.

See :doc:`../guides/histograms_yaml` for the full histogram definition guide.

