First steps
===========

YDANA-HEP is a **declarative** framework. You define *what* you want to do (your cuts, your variables, your histograms) in simple YAML configurations (see :doc:`yaml_basics`), and YDANA-HEP figures out *how* to execute it optimally using lazy evaluation. 

Prerequisite 
-------------

To get the most out of the framework, it is highly recommended to familiarize yourself with the core technologies powering it under the hood: 

* **Dask & Dask-Awkward (The Engine):** YDANA-HEP uses lazy evaluation. When you define a histogram or a cut, no data is actually read. Instead, Dask builds a "Task Graph". The data is only processed when the final ``.compute()`` is called.   

  * *Highly Recommended Reading:* `10 Minutes to Dask <https://docs.dask.org/en/stable/10-minutes-to-dask.html>`_ and the `Dask-Awkward Documentation <https://dask-awkward.readthedocs.io/>`_. 

* **Awkward Array (The Data Structure):** High Energy Physics data is jagged. Awkward Array allow NumPy-like math on these jagged structures without writing ``for`` loops.   

  * *Recommended Reading:* `Awkward Array Getting Started <https://awkward-array.org/doc/main/getting_started/index.html>`_. 

* **Coffea (The I/O):** YDANA-HEP uses Coffea's ``NanoEventsFactory`` to map flat ROOT branches into beautiful, object-oriented Awkward arrays, which behind the scena use ``uproot`` to read ROOT files directly in Python. 

Execution pipeline
------------------

To see how YDANA-HEP maps YAML files to the Dask task graph and processes data, please refer to the detailed execution flow diagram in the :doc:`../core_concepts/yaml_driven_workflow` guide. 

CLI
---

All standard workflows are driven through the ``ydana`` command-line interface.
It exposes four subcommands, each handling one stage of the pipeline:

* ``ydana dump-events`` — reads datasets through the pre-steps pipeline and writes filtered events to ROOT or Parquet files (always per partition).
* ``ydana fill-histos`` — fills the histograms declared in ``histograms.yaml`` and writes ROOT output. Supports ``--per-partition`` mode to cap peak memory.
* ``ydana merge-roots`` — merges per-partition event ROOT files into a single output file after a ``dump-events`` run.
* ``ydana merge-histos`` — merges per-partition histogram ROOT files into a single output file after a ``fill-histos --per-partition`` run.

Two first commands take ``-cf`` argument to point at your ``run_config.yaml``. All of them allows ``-o`` for the output directory.
For the full flag reference and override options, see :doc:`../guides/cli_usage`.


First analysis
--------------

You have a complete sandbox environment in the repository. Navigate to the ``examples/`` directory in your terminal. This folder contains sample ROOT files, full YAML configurations, and Python logic files. 

You can run the entire pipeline—dumping filtered events to ``ROOT/Parquet`` and filling histograms—using the provided bash script which use the CLI YDANA-HEP interface: 

.. code-block:: bash

    cd examples/
    ./run_analysis.sh --filesets

**What just happened?**

1. The CLI read ``yamls/run_config.yaml``. 
2. It lazily loaded the datasets defined in ``filesets.yaml``. 
3. It applied the cuts and preprocessors from ``pre_steps.yaml``. 
4. It built the task graph for the histograms in ``histograms.yaml``. 
5. It executed the graph and saved your outputs to the ``results/`` folder! 

For a deeper dive into how to bypass the CLI and write custom Python scripts, check out the :doc:`/examples/programmatic-api` notebook. 
