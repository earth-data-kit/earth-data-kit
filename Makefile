.PHONY: build-shared-libs run-tests build build-package install-package build-docs rebuild-docs release release-docs

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
GIT ?= git
GH ?= gh
# Prefer using TAG over tag for consistency; fall back to tag if TAG is not set.
TAG ?= $(tag)

# Install the built Python package
install-package:
	@echo "Installing package..."
	@echo "Fetching latest release tarball URL..."
	@TARBALL_URL=$$(curl -s https://api.github.com/repos/earth-data-kit/earth-data-kit/releases/latest | jq -r '.tarball_url'); \
	echo "Installing from $$TARBALL_URL"; \
	$(PIP) install "$$TARBALL_URL"

# Run tests using pytest
run-tests:
	@echo "Running tests..."
	$(PYTHON) -m pytest ./ -rx

# Build shared libraries using Go
build-shared-libs:
	@echo "Building shared libraries..."
	@echo "Bulding for macos/arm64"
	cd $(SHARED_LIBS_DIR) && env GOOS=darwin GOARCH=arm64 go build -o $(GO_LIB_OUTPUT)-darwin-arm64 main.go
	@echo "Bulding for linux/amd64"
	cd $(SHARED_LIBS_DIR) && env GOOS=linux GOARCH=amd64 go build -o $(GO_LIB_OUTPUT)-linux-amd64 main.go

# Build the Python package with Poetry
build-package:
	@echo "Building Python package..."
	$(POETRY) build

# Build project documentation using Sphinx
build-docs:
	@echo "Building documentation..."
	rm -rf $(DOCS_BUILD_DIR)/*
	$(SPHINXBUILD) -M html $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)

# Builds the shared-libs and package
build:
	@echo "Rebuilding shared libraries and package..."
	$(MAKE) build-shared-libs
	$(MAKE) build-package

# Create a new release
release:
	@echo "Creating release $(TAG)..."
	$(MAKE) build
	$(GH) release create $(TAG) --title $(TAG) --generate-notes
	$(GH) release upload $(TAG) dist/earth_data_kit-$(TAG).tar.gz

# Update GitHub Pages with the latest documentation
release-docs:
	@echo "Releasing documentation..."
	rm -rf $(GITHUB_PAGES_DIR)
	$(MAKE) build-docs
	$(GIT) clone $(GITHUB_PAGES_REPO) $(GITHUB_PAGES_DIR)
	rm -rf $(GITHUB_PAGES_DIR)/docs
	mkdir -p $(GITHUB_PAGES_DIR)/docs
	cp -R $(DOCUMENTATION_SRC)/* $(GITHUB_PAGES_DIR)/docs/
	touch $(GITHUB_PAGES_DIR)/docs/.nojekyll
	cd $(GITHUB_PAGES_DIR) && $(GIT) add . && $(GIT) commit -m "$(TAG)" && $(GIT) push origin master