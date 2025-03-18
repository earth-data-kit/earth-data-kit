Release Process
===============

This document outlines the process for creating and publishing new releases of Earth Data Kit.

Version Numbering
-----------------

Earth Data Kit follows `Semantic Versioning <https://semver.org/>`_ (SemVer):

* **MAJOR** version for incompatible API changes
* **MINOR** version for backward-compatible functionality additions
* **PATCH** version for backward-compatible bug fixes
* Alpha/beta releases are denoted with suffixes (e.g., ``1.0.0a1``, ``1.0.0b1``)

The current version is maintained in ``earth_data_kit/__init__.py`` as the ``__version__`` variable.

Release Checklist
-----------------

1. **Prepare the Release**

   * Ensure all tests pass on the release branch
   * Update the version number in ``earth_data_kit/__init__.py``
   * Update the changelog with all notable changes
   * Install the package locally for testing:
   
     .. code-block:: console
     
        $ pip3 install .
        
   * Build the package using the Makefile:
   
     .. code-block:: console
     
        $ make build
        
   * Create and merge a PR with the changelog updates

2. **Create the Release Package**

   * Clean the build environment:

     .. code-block:: console

        $ rm -rf build/ dist/ *.egg-info/

   * Build the package:

     .. code-block:: console

        $ python -m build

   * Verify the package contents:

     .. code-block:: console

        $ tar -tzf dist/earth_data_kit-X.Y.Z.tar.gz

3. **Create a GitHub Release**

   * Tag the release in git:

     .. code-block:: console

        $ git tag -a vX.Y.Z -m "Release vX.Y.Z"
        $ git push origin vX.Y.Z

   * Create a new release on GitHub
   * Upload the distribution files from the ``dist/`` directory
   * Include release notes detailing the changes

4. **Update Documentation**

   * Ensure the documentation reflects the new version
   * Update installation instructions with the new version number
   * Rebuild and deploy the documentation

5. **Announce the Release**

   * Notify the team and users about the new release
   * Highlight key features, improvements, and bug fixes

Docker Image Updates
-------------------

When releasing a new version, update the Docker image:

1. Update the Dockerfile with the new package version
2. Build and tag the new Docker image:

   .. code-block:: console

      $ docker build -t earth-data-kit:vX.Y.Z .

3. Push the image to the container registry:

   .. code-block:: console

      $ docker push earth-data-kit:vX.Y.Z
      $ docker push earth-data-kit:latest

Post-Release
-----------

After completing a release:

1. Increment the version number in the development branch to the next anticipated version with a development suffix (e.g., ``X.Y+1.0.dev0``)
2. Create an issue for planning the next release
