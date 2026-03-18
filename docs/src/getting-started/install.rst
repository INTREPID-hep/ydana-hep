Installation
============

YDANA-HEP requires Python 3.9 or newer. Most users do **not** need to clone the
repository just to install the package and run the CLI. If you need the full 
contributor workflow, local docs builds, or editable development installs,
see ``CONTRIBUTING.md`` in the repository root.

.. note::

   Any other Python environment manager should work as well, but the following instructions use
   ``uv`` for simplicity.


Create a clean virtual environment and install YDANA-HEP directly from GitHub:

.. code-block:: bash

   uv venv
   uv pip install "git+https://github.com:uniovi-hepex/ydana-hep/ydana-hep.git"

This gives you the ``ydana`` CLI without requiring a local checkout.

To install a specific tagged release instead of the default branch:

.. code-block:: bash

   uv pip install "git+https://github.com:uniovi-hepex/ydana-hep/ydana-hep.git@[TAG_VERSION]"


Local Repository Setup
~~~~~~~~~~~~~~~~~~~~~~

Clone the repository only if you want one of these:

* editable installs while modifying source code;
* the bundled ``examples/`` scripts, notebooks, and YAML configs;
* local documentation builds;
* contributor and test workflows.

For that case:

.. code-block:: bash

   git clone https://github.com:uniovi-hepex/ydana-hep/ydana-hep.git
   cd ydana
   uv venv
   uv sync --extra dev

If you also want the dev and docs dependencies:

.. code-block:: bash

   uv sync --extra dev --extra docs

If you specifically want an editable install from the repo checkout:

.. code-block:: bash

   uv pip install -e .


Verify the Installation
~~~~~~~~~~~~~~~~~~~~~~~

You can verify the CLI is available:

.. code-block:: bash

   ydana --help

You can also confirm the package metadata:

.. code-block:: bash

   python -m pip show ydana-hep
