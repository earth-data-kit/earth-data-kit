.PHONY: build-shared-libs run-tests build-package install-package build-docs rebuild-docs release release-docs

# Variables
SHARED_LIBS_DIR := earth_data_kit/stitching/shared_libs/earth_data_kit
GO_LIB_OUTPUT := ../builds/go-lib
PYTHON ?= python3
PIP ?= pip3
POETRY ?= poetry
SPHINXBUILD ?= sphinx-build
DOCS_SOURCE_DIR := docs/source
DOCS_BUILD_DIR := docs/build
DOCUMENTATION_SRC := $(DOCS_BUILD_DIR)/html
TMP_DIR := tmp
GITHUB_PAGES_REPO := git@github.com:earth-data-kit/earth-data-kit.github.io.git
GITHUB_PAGES_DIR := $(TMP_DIR)/earth-data-kit.github.io
GIT ?= git
GH ?= gh
# Prefer using TAG over tag for consistency; fall back to tag if TAG is not set.
TAG ?= $(tag)

# Build shared libraries using Go
build-shared-libs:
	@echo "Building shared libraries..."
	cd $(SHARED_LIBS_DIR) && go build -o $(GO_LIB_OUTPUT) main.go

# Run tests using pytest
run-tests:
	@echo "Running tests..."
	$(PYTHON) -m pytest ./ -rx

# Build the Python package with Poetry
build-package:
	@echo "Building Python package..."
	$(POETRY) build

# Install the built Python package
install-package:
	@echo "Installing package..."
	$(PIP) install dist/earth_data_kit-1.0.0a1.tar.gz

# TODO: Test this
# Build project documentation using Sphinx
build-docs:
	@echo "Building documentation..."
	rm -rf $(DOCS_BUILD_DIR)/*
	$(SPHINXBUILD) -M html $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)

# Rebuild the package and documentation
rebuild-docs:
	@echo "Rebuilding package and documentation..."
	$(MAKE) build-package
	$(MAKE) install-package
	$(MAKE) build-docs

# TODO: Test this
# Create a new release: build shared libraries, docs, and create GitHub release
release:
	@echo "Creating release $(TAG)..."
	$(MAKE) build-shared-libs
	$(MAKE) rebuild-docs
	$(GH) release create $(TAG) --title $(TAG) --generate-notes
	$(GH) release upload $(TAG) dist/earth_data_kit-$(TAG).tar.gz

# TODO: Test this
# Update GitHub Pages with the latest documentation
release-docs:
	@echo "Releasing documentation..."
	rm -rf $(TMP_DIR)
	$(MAKE) rebuild-docs
	$(GIT) clone $(GITHUB_PAGES_REPO) $(GITHUB_PAGES_DIR)
	rm -rf $(GITHUB_PAGES_DIR)/docs
	mkdir -p $(GITHUB_PAGES_DIR)/docs
	cp -R $(DOCUMENTATION_SRC)/* $(GITHUB_PAGES_DIR)/docs/
	touch $(GITHUB_PAGES_DIR)/docs/.nojekyll
	cd $(GITHUB_PAGES_DIR) && $(GIT) add . && $(GIT) commit -m "$(TAG)" && $(GIT) push origin master