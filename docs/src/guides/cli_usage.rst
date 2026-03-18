CLI usage
=========

The ``ydana`` CLI is powered by :mod:`ydana.cli` and exposes four subcommands.
Run ``ydana --help`` or ``ydana <command> --help`` for the authoritative
argument listing.

.. contents::
   :local:
   :depth: 1


``ydana dump-events``
---------------------

Reads datasets through the pre-steps pipeline and writes events to ROOT or
Parquet.

.. code-block:: bash

   ydana dump-events -cf run_config.yaml -o results/

   # Explicit inputs instead of filesets
   ydana dump-events -cf run_config.yaml \
       --inputs "/data/*.root" \
       --tree   "dtNtupleProducer/DTTREE" \
       -o results_DY --tag _DY

Key flags:

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Flag
     - Description
   * - ``-cf / --config-file``
     - Path to ``run_config.yaml`` (required).
   * - ``-o / --outpath``
     - Output directory. Created if it does not exist.
   * - ``--inputs``
     - Glob or list of files, overrides ``filesets.yaml``.
   * - ``--tree``
     - ROOT TTree path, e.g. ``"dtNtupleProducer/DTTREE"``.
   * - ``--tag``
     - String appended to output file names.


``ydana fill-histos``
---------------------

Reads datasets through the pre-steps pipeline and fills the histograms declared in ``histograms.yaml`` and writes ROOT files.

.. code-block:: bash

   ydana fill-histos -cf run_config.yaml -o results/ # also supports explicit inputs and tree as in dump-events

   # Per-partition mode (recommended for large datasets)
   ydana fill-histos -cf run_config.yaml -o results/ --per-partition


Key flags:

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Flag
     - Description
   * - ...
     - Same flags as ``dump-events`` for inputs, tree, etc.
   * - ``--per-partition``
     - Materialise and write each Dask partition separately (lower peak RAM).


``ydana merge-roots``
---------------------

Combines multiple per-partition ROOT event files into a single file. It is supported by ROOT ``hadd``, so, 
ensure to have it accessible in your environment.

.. code-block:: bash

   ydana merge-roots \
       -i "results/roots/*.root" \
       -o results/ --tag _merged

Key flags:

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Flag
     - Description
   * - ``-i / --inputs``
     - Glob pattern or list of ROOT files to merge.
   * - ``-o / --outpath``
     - Output directory.
   * - ``--tag``
     - Suffix for the merged output file name.


``ydana merge-histos``
----------------------

Combines multiple per-partition histogram ROOT files into one.

.. code-block:: bash

   ydana merge-histos \
       -i "results/histograms/*.root" \
       -o results/ --tag _merged

The flags are identical to ``merge-roots`` above.


Full end-to-end example
-----------------------

The complete workflow for two datasets (DY and Z'):

.. code-block:: bash

   cd examples/

   # Step 1 — dump events
   ydana dump-events -cf yamls/run_config.yaml -o results/

   # Step 2 — fill histograms per-partition
   ydana fill-histos -cf yamls/run_config.yaml -o results/ --per-partition

   # Step 3 — merge by dataset
   ydana merge-roots  -i "results/roots/DY_*.root"     -o results/ --tag _DY
   ydana merge-histos -i "results/histograms/DY_*.root" -o results/ --tag _DY


See :doc:`../getting-started/first_steps` for the ``run_analysis.sh`` walkthrough that calls all these commands automatically.

