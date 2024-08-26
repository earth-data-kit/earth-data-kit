Getting Started
===============

Installation
------------

To use Earth Data Kit, first download the latest release from https://github.com/siddhantgupta3/earth-data-kit/releases/ and install it using tarball:

.. code-block:: console
   
   (.venv) $ wget https://github.com/siddhantgupta3/earth-data-kit/releases/download/1.0.0a1/earth_data_kit-1.0.0a1.tar.gz 
   (.venv) $ pip3 install earth_data_kit-1.0.0a1.tar.gz

Environment Setup
-----------------

Environment variables can be used to configure different aspects of earth data kit, like AWS credentials, GDAL options, etc.
You can use ``.env`` file and ``python-dotenv`` package to configure edk.

Below is a list of all possible options

**General options**

* ``TMP_DIR`` *(Required)*: Directory to stores temporary files, example scene files, pre-processed vrts, etc.

**AWS related options**

* ``AWS_REGION`` : The region to use, eg: us-west-2
* ``AWS_NO_SIGN_REQUEST`` (YES/NO): Do not sign requests. Credentials will not be loaded if this argument is provided.
* ``AWS_REQUEST_PAYER`` (requester): Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests


**GDAL related options**

* ``GDAL_HTTP_TCP_KEEPALIVE`` (YES/NO): Sets whether to enable TCP keep-alive. Defaults to NO

