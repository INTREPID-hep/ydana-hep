Lazy Execution: Triggering the Graph
====================================

.. contents::
   :local:
   :depth: 1


As seen in :doc:`lazy_loading`, YDANA-HEP initializes datasets and applies cuts by extending a 
``dask-awkward`` task graph, completely avoiding immediate disk I/O. Lazy execution is the concept 
of deferring all computation until a *terminal action* explicitly requests the final result. 


By waiting until the very end to execute, Dask can optimize the entire analysis pipeline at once.

.. mermaid::

    flowchart LR
        subgraph Eager ["Eager (NumPy / standard awkward)"]
            direction LR
            EA[("Load Data")] -->|Loads into RAM| EB("Apply Cuts")
            EB -->|RAM| EC("Fill Histogram")
            EC -->|RAM| ED[("Write ROOT File")]
        end

        subgraph Lazy ["Lazy (YDANA-HEP / dask-awkward)"]
            direction LR
            LA["NTuple()"] -->|Extends graph| LB("pre-steps pipeline")
            LB -->|Extends graph| LC("Histogram.fill()")
            LC -->|Extends graph| LD{"Terminal Action<br>e.g., .compute()"}
            LD -.->|Single Scheduler Call<br>Reads, transforms, fills| LE[("Partitioned Output")]
        end

        classDef engine fill:#e1f5fe,stroke:#81d4fa,stroke-width:2px,color:#000000;
        classDef eager fill:#f5f5f5,stroke:#bdbdbd,stroke-width:2px,color:#000000;
        classDef action fill:#fff3e0,stroke:#ffb74d,stroke-width:2px,color:#000000;

        class LA,LB,LC engine;
        class LD action;
        class LE engine;
        class EA,EB,EC,ED eager;


With lazy execution, you gain three massive advantages:

* **Bounded Memory:** Only the data for the current partition lives in RAM.
* **Graph Fusion:** Dask fuses operations before execution. This reduces redundant I/O and intermediate allocations by intelligently reading *only* the ROOT branches that actually survive to the end of your graph.
* **Distributed Scaling:** The exact same task graph can be submitted to a Dask distributed cluster without any code changes.


How to Trigger Execution
------------------------

The graph remains dormant until you hit a terminal action. This can happen either via the Python Programmatic API or the CLI.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Action (Trigger)
     - What it does
   * - ``array.compute()``
     - Python API: Triggers single-array materialisation.
   * - ``dask.compute(a, b, h.h)``
     - Python API: The recommended method. Triggers a joint optimised compute of all passed objects simultaneously.
   * - ``ydana fill-histos``
     - CLI Command: Triggers compute to fill histograms and write outputs (behavior depends on execution mode flags).
   * - ``ydana dump-events``
     - CLI Command: Triggers compute and dumps the filtered event partitions to disk.


See Also
--------

* :doc:`dask_task_graphs` — visual of the full end-to-end task graph.
