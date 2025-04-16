Release Process
===============

This document outlines the process for creating and publishing new releases of Earth Data Kit. Earth Data Kit versioning starts from 0.1.0 and follows a straightforward versioning scheme. We don't use alpha or beta releases in our versioning strategy.

Version Numbering
-----------------

Earth Data Kit follows `Semantic Versioning <https://semver.org/>`_ (SemVer):

* **MAJOR** version for incompatible API changes
* **MINOR** version for backward-compatible functionality additions
* **PATCH** version for backward-compatible bug fixes

The current version is maintained in ``earth_data_kit/__init__.py`` as the ``__version__`` variable.

Planning the Release
--------------------

1. **Development Workflow**

   * All development work happens on the ``dev`` branch
   * Features and fixes are developed in feature branches and merged into ``dev``
   * Code reviews and tests are performed before merging into ``dev``

2. **Release Branch Creation**

   * When approaching a planned release, create a release branch from ``dev``:

     .. code-block:: console

        $ git checkout dev
        $ git pull
        $ git checkout -b release/vX.Y.Z

   * The release branch is used to prepare and stabilize the release
   * Only bug fixes and release-specific changes should be made on this branch
   * Any changes made to the release branch must be merged back to ``dev`` after release

3. **Release Preparation**

   * Finalize features and fixes for the release
   * Make sure all documentation is updated to reflect the new version
   * Ensure all tests are passing:

     .. code-block:: console

        $ make run-tests

   * Update version using the make command:

     .. code-block:: console

        $ make bump-version # You will be prompted to enter version type (patch, minor, major)
   * Update the changelog with all notable changes
   * Merge all changes to the ``dev`` branch

Release Checklist
-----------------

1. **Finalize the Release**

   * Create a pull request to merge the ``dev`` branch to ``master``:
     
     1. Create the pull request with a descriptive title and details about the release
     2. Make sure all tests pass
     3. Once approved, merge the pull request

   * Lock the release branch to prevent further changes
   * Checkout the master branch for release:

     .. code-block:: console

        $ git checkout master
        $ git pull

2. **Build and Release the Package and Documentation**

   * Clean the build environment:

     .. code-block:: console

        $ rm -rf build/ dist/

   * Build the package:

     .. code-block:: console

        $ make build

   * Install the built package to ensure documentation builds with the latest code:

     .. code-block:: console

        $ pip3 install dist/earth_data_kit-*.tar.gz

   * Build the documentation:

     .. code-block:: console

        $ make build-docs

   * Release both the package and documentation using the tag. Note that the tag is the version number without the ``v`` prefix:

     .. code-block:: console

        $ TAG=X.Y.Z make release
        $ TAG=X.Y.Z make release-docs

4. **Announce the Release**

   * Notify the team and users about the new release
   * Include a link to the changelog
   * Highlight key features, improvements, and bug fixes

Development Releases
--------------------

For development releases, follow these steps:

1. **Create a Development Version**

   * After merging changes that need to be tested in a development environment, create a development version:

     .. code-block:: console

        $ make bump-dev

     This will create a version like ``X.Y.Z.devYYYYMMDD`` based on the current version.

2. **Build and Release the Development Package**
   
   * Build the package:

     .. code-block:: console

        $ make build
   
   * Install the development package:

     .. code-block:: console

        $ pip install dist/earth_data_kit-*.tar.gz
   
   * Build the documentation:

     .. code-block:: console

        $ make build-docs

   * Create a GitHub release with the development tag:

     .. code-block:: console

        $ TAG=$(poetry version -s) make release

   * Release the development documentation (hosted at https://earth-data-kit.github.io/dev-docs):

     .. code-block:: console

        $ TAG=$(poetry version -s) make release-dev-docs

.. note::
   Development releases use version numbers that are lower than stable releases (e.g. ``0.1.2.dev20240501`` < ``0.1.2``), ensuring proper upgrades.
