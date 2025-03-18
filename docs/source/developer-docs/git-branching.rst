Git Branching
=============

Earth Data Kit follows a structured Git branching strategy that combines elements of Git Flow with Release Branching for stability and maintainability.

Branch Structure
---------------

* **master**: Contains only stable, production-ready code
* **develop**: Active development branch where features are integrated
* **feature branches** (``feature-xyz``): Created from ``develop``, merged back after completion
* **release branches** (``release-x.y.z``): Created from ``develop`` for release preparation
* **hotfix branches** (``hotfix-x.y.z``): Created from ``master`` for urgent fixes
* **long-term release branches** (``release-x.y.z``, ``release-x.y.z``): Maintained for LTS versions. Release branches are merged into develop and master.

Workflow
--------

1. **Feature Development**

   * Create a feature branch from ``develop``:
   
     .. code-block:: console
     
        $ git checkout develop
        $ git checkout -b feature-new-functionality
        
   * Implement and test the feature
   * Create a pull request to merge back into ``develop``

2. **Preparing a Release**

   * Create a release branch from ``develop`` when ready:
   
     .. code-block:: console
     
        $ git checkout develop
        $ git checkout -b release-1.2.0
        
   * Run the test suite to ensure everything works correctly:
   
     .. code-block:: console
     
        $ make run-tests
        
   * Update the version number using poetry (increment major, minor, patch, or prerelease as appropriate):
   
     .. code-block:: console
     
        $ poetry version minor      # For minor version bump (e.g., 1.1.0 to 1.2.0)
        $ poetry version major      # For major version bump (e.g., 1.0.0 to 2.0.0)
        $ poetry version patch      # For patch version bump (e.g., 1.1.0 to 1.1.1)
        $ poetry version prerelease # For prerelease version (e.g., 1.1.0 to 1.1.0-alpha.1)
        
   * Install the package locally:
   
     .. code-block:: console
     
        $ pip install -e .
        
   * Build the package (this will also build the documentation):
   
     .. code-block:: console
     
        $ make build
        
   * Merge into both ``master`` and ``develop`` when ready

3. **Hotfixes**

   * For urgent production fixes, branch from ``master``:
   
     .. code-block:: console
     
        $ git checkout master
        $ git checkout -b hotfix-1.2.1
        
   * Fix the issue and test thoroughly
   * Update the version number in ``earth_data_kit/__init__.py`` (increment the PATCH version)
   * Update the changelog to document the fix
   * Commit the version bump and changelog updates:
   
     .. code-block:: console
     
        $ git commit -am "Bump version to 1.2.1 for hotfix"
        
   * Build and test the package:
   
     .. code-block:: console
     
        $ make build
        $ make run-tests
        
   * Merge back into both ``master`` and ``develop``
   * Create and publish the release:
   
     .. code-block:: console
     
        $ make release TAG=1.2.1
        
   * Update documentation if needed:
   
     .. code-block:: console
     
        $ make release-docs TAG=1.2.1

4. **Long-term Support**

   * Use release branches for long-term support of older versions
   * Use the same version bumping and release process as with hotfixes


5. **Creating a Release**

   * Once code is ready for release in the ``master`` branch:
   
     .. code-block:: console
     
        $ git checkout master
        $ git pull origin master
        
   * Ensure the version in ``earth_data_kit/__init__.py`` is correct
   * Tag the release:
   
     .. code-block:: console
     
        $ git tag -a v1.2.3 -m "Release version 1.2.3"
        $ git push origin v1.2.3
        
   * Build the release package:
   
     .. code-block:: console
     
        $ rm -rf build/ dist/ *.egg-info/
        $ python -m build
        
   * Create a GitHub release:
     - Go to the repository's Releases page
     - Click "Draft a new release"
     - Select the tag you just pushed
     - Add release notes detailing changes
     - Upload the distribution files from the ``dist/`` directory
     
   * Publish to PyPI (if applicable):
   
     .. code-block:: console
     
        $ python -m twine upload dist/*
        
   * Update documentation:
   
     .. code-block:: console
     
        $ make docs
        $ make publish-docs
        
   * Announce the release to the team and users

