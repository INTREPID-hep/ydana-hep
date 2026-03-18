"""Sphinx configuration for YDANA documentation."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

project = "YDANA-HEP"
copyright = "2026, Daniel Estrada, Universidad de Oviedo"
author = "Daniel Estrada"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
    "nbsphinx",
]

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**/.ipynb_checkpoints",
    "**/_*.rst",
]

autosummary_generate = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

# Avoid importing optional heavy runtime stacks during doc builds.
autodoc_mock_imports = [
    "coffea",
    "numba",
    "coverage",
]

napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

nbsphinx_execute = "never"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "awkward": ("https://awkward-array.org/doc/main/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
}

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_show_sourcelink = True
rst_prolog = """
:github_url: https://github.com:uniovi-hepex/ydana-hep/ydana-hep
"""
