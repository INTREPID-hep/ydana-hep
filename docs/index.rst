YDANA-HEP Documentation
=======================

**Y**\AML-Driven **D**\ask - **A**\wkward **N**\tuple **A**\nalyzer is a configuration-driven 
framework for HEP (High Energy Physics) columnar data analysis built around lazy arrays and distributed execution.

It combines the object-oriented event structures of Coffea's ``NanoEventsFactory`` with the distributed 
processing power of ``dask-awkward``. The main goal of YDANA is to provide a **declarative** workflow where 
datasets, schema mappings, preprocessing steps, and histograms are configured directly from YAML files, 
reducing the need to write custom execution scripts.

.. toctree::
   :maxdepth: 2

   src/getting-started/index

.. toctree::
   :maxdepth: 2

   src/core_concepts/index

.. toctree::
   :maxdepth: 2
   :caption: Guides

   src/guides/index

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   src/reference/index

.. toctree::
   :maxdepth: 1
   :caption: Troubleshooting

   src/troubleshooting/index
