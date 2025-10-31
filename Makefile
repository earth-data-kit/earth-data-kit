.PHONY: run-tests build-docs prettify bump-dev bump-version

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
GITHUB_PAGES_DIR := build/earth-data-kit.github.io
DEV_GITHUB_PAGES_REPO := git@github.com:earth-data-kit/dev-docs.git
DEV_GITHUB_PAGES_DIR := build/dev-docs
GIT ?= git
GH ?= gh
# Prefer using TAG over tag for consistency; fall back to tag if TAG is not set.
TAG ?= $(tag)

# Run tests using pytest
run-tests:
	@echo "Running tests..."
	$(PYTHON) -m pytest ./ -rx

# Build project documentation using Sphinx
build-docs:
	@echo "Building documentation..."
	rm -rf $(DOCS_BUILD_DIR)/*
	$(SPHINXBUILD) -M html $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)

# Format Python code with Black
prettify:
	@echo "Formatting Python code with Black..."
	@black .
	@echo "\033[0;32mPython code formatted successfully.\033[0m"

# Bump version for development
bump-dev:
	@echo "Bumping to development version..."
	@VERSION=$$($(POETRY) version -s) && \
	DEV_VERSION="$${VERSION}.dev$$(date +%Y%m%d)" && \
	echo "Updating version to $${DEV_VERSION}..." && \
	$(POETRY) version $${DEV_VERSION} && \
	echo "\033[0;32mVersion bumped to $${DEV_VERSION}\033[0m"

# Bump version (patch, minor, major)
bump-version:
	@echo "Bumping version..."
	@read -p "Enter version type (patch, minor, major): " VERSION_TYPE && \
	NEW_VERSION=$$($(POETRY) version $${VERSION_TYPE} -s) && \
	echo "\033[0;32mVersion bumped to $${NEW_VERSION}\033[0m"
