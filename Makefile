.PHONY: build-shared-libs run-tests build build-package install-package build-docs rebuild-docs release release-docs prettify

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

# Install the built Python package
install-package:
	@echo "Installing EDK..."
	@echo "Fetching latest release tarball URL..."
	@TARBALL_URL=$$(curl -s https://api.github.com/repos/earth-data-kit/earth-data-kit/releases/latest | jq -r '.assets[0].browser_download_url'); \
	VERSION_NUM=$$(curl -s https://api.github.com/repos/earth-data-kit/earth-data-kit/releases/latest | jq -r '.tag_name'); \
	if [ -z "$$VERSION_NUM" ] || [ "$$VERSION_NUM" = "null" ]; then \
		echo "\033[0;31mError: Unable to fetch the latest version information.\033[0m"; \
		exit 1; \
	fi; \
	echo "Found version $$VERSION_NUM from $$TARBALL_URL"; \
	$(PIP) install "$$TARBALL_URL" --no-cache-dir; \
	echo "\033[0;32mYou can now use EDK $$VERSION_NUM in your Python environment.\033[0m"

# Run tests using pytest
run-tests:
	@echo "Running tests..."
	$(PYTHON) -m pytest ./ -rx

# Build shared libraries using Go
build-shared-libs:
	@echo "Building shared libraries..."
	@echo "Bulding for macos/arm64"
	@cd $(SHARED_LIBS_DIR) && env GOOS=darwin GOARCH=arm64 go build -o $(GO_LIB_OUTPUT)-darwin-arm64 main.go
	@echo "Bulding for linux/amd64"
	@cd $(SHARED_LIBS_DIR) && env GOOS=linux GOARCH=amd64 go build -o $(GO_LIB_OUTPUT)-linux-amd64 main.go
	@echo "\033[0;32mShared libraries built successfully.\033[0m"

# Build the Python package with Poetry
build-package:
	@echo "Building Python package..."
	@$(POETRY) build -q
	@echo "\033[0;32mPython package built successfully.\033[0m"

# Build project documentation using Sphinx
build-docs:
	@echo "Building documentation..."
	rm -rf $(DOCS_BUILD_DIR)/*
	$(SPHINXBUILD) -M html $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)

# Builds the shared-libs and package
build:
	@$(MAKE) build-shared-libs
	@echo ""
	@$(MAKE) build-package

# Create a new release
release:
	@echo "Creating release $(TAG)..."
	$(GH) release create $(TAG) --title $(TAG) --generate-notes
	$(GH) release upload $(TAG) dist/earth_data_kit-$(TAG).tar.gz

# Update GitHub Pages with the latest documentation
release-docs:
	@echo "Releasing documentation..."
	rm -rf $(GITHUB_PAGES_DIR)
	$(GIT) clone $(GITHUB_PAGES_REPO) $(GITHUB_PAGES_DIR)
	rm -rf $(GITHUB_PAGES_DIR)/docs
	mkdir -p $(GITHUB_PAGES_DIR)/docs
	cp -R $(DOCUMENTATION_SRC)/* $(GITHUB_PAGES_DIR)/docs/
	touch $(GITHUB_PAGES_DIR)/docs/.nojekyll
	cd $(GITHUB_PAGES_DIR) && $(GIT) add . && $(GIT) commit -m "$(TAG)" && $(GIT) push origin master

release-dev-docs:
	@echo "Releasing development documentation..."
	rm -rf $(DEV_GITHUB_PAGES_DIR)
	$(GIT) clone $(DEV_GITHUB_PAGES_REPO) $(DEV_GITHUB_PAGES_DIR)
	rm -rf $(DEV_GITHUB_PAGES_DIR)/docs
	mkdir -p $(DEV_GITHUB_PAGES_DIR)/docs
	cp -R $(DOCUMENTATION_SRC)/* $(DEV_GITHUB_PAGES_DIR)/docs/
	touch $(DEV_GITHUB_PAGES_DIR)/docs/.nojekyll
	cd $(DEV_GITHUB_PAGES_DIR) && $(GIT) add . && $(GIT) commit -m "$(TAG)" && $(GIT) push origin master

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
