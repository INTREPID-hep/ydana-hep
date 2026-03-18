YAML-Driven Workflow
====================

YAML files encode what to process, how to transform events, and what to export. This keeps analysis behaviour declarative and reproducible across local and distributed runs. 

.. contents::
   :local:
   :depth: 1


Full workflow map
-----------------

The following diagram illustrates how the YAML configuration translates into lazy arrays, and how the execution branches into either Programmatic Python access or CLI-driven commands. 

.. mermaid::

    graph TD
        A[run_config.yaml] -->|Loads| B(filesets.yaml)
        A -->|Loads| C(schema.yaml)
        A -->|Loads| D(pre_steps.yaml)
        A -->|Loads| E(histograms.yaml)
        
        B --> F[YDANA-HEP NTuple Loader]
        C --> F
        D -->|Injects Lazy Cuts/Mutations| F
        
        F -->|Reads Metadata| G[(ROOT Files)]
        G --> Z[Coffea / uproot lazy reading]
        Z -->|Applies cuts & creates| H{{Lazy dask_awkward Array}}
        
        %% Path 1: Python Level Access
        H -->|Path 1: Python API| PY[Jupyter / Custom Scripts]
        PY -->|Manual Graph Building| I[dask.compute]
        
        %% Path 2: CLI Executions
        H -->|Path 2: CLI Execution| CLI{YDANA-HEP CLI Commands}
        E -->|Defines Lazy Histograms| CLI
        
        CLI -->|ydana dump-events| I
        CLI -->|ydana fill-histos| I
        
        I -->|Triggers Distributed I/O| G
        I --> J[Output: Partitioned ROOT/Parquet]
        
        J -->|ydana merge-roots / merge-histos| K[Merged Final Outputs]

        classDef yaml fill:#f9f2f4,stroke:#d3b6c6,stroke-width:2px,color:#000000;
        classDef engine fill:#e1f5fe,stroke:#81d4fa,stroke-width:2px,color:#000000;
        classDef cli fill:#fff3e0,stroke:#ffb74d,stroke-width:2px,color:#000000;
        class A,B,C,D,E yaml;
        class H,I,Z engine;
        class CLI cli;

Key Properties
--------------

**Declarative**
    No Python code is required to run a standard analysis. The YAML files fully describe inputs, transforms, and outputs. 
**Composable**
    Each YAML section can be ``!include``-d from a separate file, allowing modular and version-controlled configs. 
**Reproducible**
    The same config file produces identical output regardless of when or where it is run.
**Extensible**
    Custom selectors and preprocessors can be written in Python and referenced by dotted import path (``src: mymodule.my_fn``) without modifying YDANA-HEP itself. 

Config Object
-------------

At runtime, the YAML tree is parsed into a :class:`ydana.base.config.Config` object, which is stored as a module-level singleton ``RUN_CONFIG`` via :func:`ydana.base.config.set_run_config` and accessed anywhere via :func:`ydana.base.config.get_run_config`. This means any component (CLI, NTuple, pipeline, histograms) can read the active config without being passed it explicitly. 

Overriding at the CLI
---------------------

Most YAML settings can be overridden without editing the file: 

.. code-block:: bash

   # Use a different config file
   ydana fill-histos -cf /custom/path/run_config.yaml -o results/

   # Override dataset inputs entirely
   ydana fill-histos -cf run_config.yaml \
       --inputs "/data/*.root" --tree "Events" -o results/

See :doc:`../guides/cli_usage` for the full CLI reference.