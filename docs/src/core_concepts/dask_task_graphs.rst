Dask Task Graphs
================

Every YDANA-HEP analysis is expressed as a Dask task graph before a single byte of
data is read.

.. contents::
   :local:
   :depth: 1


End-to-End Pipeline View
------------------------

.. mermaid::

   flowchart TD
       RC["run_config.yaml"] --> CFG["Config object"]
       CFG --> NT["NTuple\n— dak.Array per dataset"]
       NT --> PS["pre-steps pipeline\n(selectors + preprocessors)"]
       PS --> HF["Histogram.fill()\n— histogram task nodes"]
       HF --> CMP["dask.compute() / scheduler"]
       CMP --> WRT["write .root files"]


Per-Partition Execution
-----------------------

When ``--per-partition`` is used the graph is sliced differently:

.. mermaid::

   flowchart LR
       DS["dak.Array"] --> D0["partition 0\n(delayed)"]
       DS --> D1["partition 1\n(delayed)"]
       DS --> Dn["partition N\n(delayed)"]
       D0 --> W0["fill + write\n_0.root"]
       D1 --> W1["fill + write\n_1.root"]
       Dn --> Wn["fill + write\n_N.root"]
       W0 --> MR["merge-histos"]
       W1 --> MR
       Wn --> MR
       MR --> OUT["merged.root"]

Each partition runs (and writes) independently. If one worker fails, only that
partition needs to be re-run.


Shared Sub-Graphs
-----------------

When multiple histograms read the same branch, Dask automatically deduplicates
the read task. All histograms that call ``events["genmuons"]["pt"]`` share one
read node in the optimised graph, avoiding repeated I/O.


Inspecting the Graph
--------------------

To visualise the task graph for a specific array:

.. code-block:: python

   from ydana.base.ntuple import NTuple

   ntuple = NTuple(datasets=["DY"])
   arr = ntuple.events["DY"]["genmuons"]["pt"]
   arr.visualize("graph.png")  # requires graphviz


See Also
--------

* :class:`ydana.base.histos.Histogram` — histogram wrapper and fill mechanics.
* :mod:`ydana.base.pipeline` — step ordering and topological sort.
* :doc:`lazy_loading` — how ``NTuple`` creates the initial graph.
* :doc:`lazy_execution` — when and how the graph executes.

