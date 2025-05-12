Getting Started
===============

Prerequisites
-------------
Before using Earth Data Kit, ensure that the following are installed:

- Python 3.12 or newer
- GDAL 3.8.4 or above - https://gdal.org/en/stable/download.html#binaries
- s5cmd (optional) - https://github.com/peak/s5cmd - Only required if you plan to query data from S3 buckets or export to S3

Installation
------------
To install Earth Data Kit, follow these steps:

1. Clone the GitHub repository:

   .. code-block:: console

      $ git clone https://github.com/earth-data-kit/earth-data-kit.git
      $ cd earth-data-kit

2. Switch to the master branch:

   .. code-block:: console

      $ git checkout master

3. Run the installation script:

   .. code-block:: console

      $ bash install.sh

This will check prerequisites, download the latest tarball from GitHub releases, and install EDK automatically.


Environment Configuration
-------------------------
Earth Data Kit can be customized via environment variables. This approach lets you easily configure settings such as AWS credentials, GDAL options, and other operational parameters. You can maintain these settings in a ``.env`` file and load them using the ``python-dotenv`` package.

General Options
~~~~~~~~~~~~~~~
* ``TMP_DIR`` *(Required)*: The directory path used for storing temporary files (e.g., catalog, pre-processed VRTs).
* ``EDK_MAX_WORKERS``: The maximum number of workers to use for parallel processing. If not set, it will use ``num_cores - 2`` for CPU intensive tasks and ``(2 * num_cores) - 1`` for I/O intensive tasks.

AWS Options
~~~~~~~~~~~
* ``AWS_REGION``: Specifies the AWS region where the data is stored (e.g., us-west-2). Use the region data is stored in when downloading data from AWS S3.
* ``AWS_NO_SIGN_REQUEST`` (YES/NO): If set to YES, this option disables request signing, meaning AWS credentials will be bypassed.
* ``AWS_REQUEST_PAYER`` (requester): Indicates that the requester accepts any charges that may result from the request. Use this when accessing buckets that require payer confirmation.

Google Earth Engine Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~
* ``GOOGLE_APPLICATION_CREDENTIALS``: Specifies the path to the JSON credentials file for authenticating with the Earth Engine API.

GDAL Options
~~~~~~~~~~~~
* ``GDAL_HTTP_TCP_KEEPALIVE`` (YES/NO): Determines whether to enable TCP keep-alive for GDAL HTTP connections (defaults to NO).

This configuration setup provides flexibility to adapt Earth Data Kit to your specific environment and processing needs.
