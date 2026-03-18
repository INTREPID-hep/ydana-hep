# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SPHINXAUTOBUILD ?= sphinx-autobuild
LIVEHTML_PORT ?= 8000
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS)

.PHONY: help Makefile livehtml clean

# Catch-all target: route all unknown targets to Sphinx's make-mode.
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS)

# Live-reload server (the usual sphinx-autobuild workflow)
livehtml:
	@$(SPHINXAUTOBUILD) "$(SOURCEDIR)" "$(BUILDDIR)/html" \
		--port $(LIVEHTML_PORT)

clean:
	@rm -rf "$(BUILDDIR)"
