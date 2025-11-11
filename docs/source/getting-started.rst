Getting Started
===============

Primary Usage: edk-cli
----------------------
The recommended and primary way to run Earth Data Kit (EDK) is via edk-cli. This approach avoids dependency issues and ensures a consistent environment across platforms.

Requirements
------------

* Python 3.12 or newer
* Docker

Quick Start
-----------
1. **Get and install edk-cli:**

   .. code-block:: console

      $ pip3 install https://github.com/earth-data-kit/edk-cli/releases/download/0.1.0/edk_cli-0.1.0-py3-none-any.whl

2. **Create your `.env` file using edk configure:**
   
   .. code-block:: console

         $ edk configure

   This will help you create a `.env` file. See the "Environment Configuration" section below for details on available options.

   .. note::
      Use relative paths (not absolute paths) when specifying directories.

      - Example: `./workspace`
      
      - Avoid: `/Users/username/earth-data-kit/workspace`

3. **Initialize the EDK container:**

   .. code-block:: console

      $ edk run

   This will build and start the EDK Docker container with all dependencies pre-installed.  
   Additionally, if you have a `requirements.txt` file inside the `workspace` directory, it will be installed automatically inside the container.

5. **SSH into the container:**

   If you want an interactive shell inside the container, run:

   .. code-block:: console

      $ edk ssh

   This will open a bash shell inside the EDK container, allowing you to run commands interactively.
   
6. **(Optional) Start a JupyterLab server inside the container:**

   If you want to use JupyterLab for interactive development, you can launch a JupyterLab server inside the EDK container by running:

   .. code-block:: console

      $ edk notebook

   This will start a JupyterLab server accessible from your browser. By default, it will be available at `http://localhost:8888` on your host machine. You can then open notebooks and interact with your code and data directly within the container environment.

For more practical usage, check out the `examples` folder in the repository:  
https://github.com/earth-data-kit/earth-data-kit/tree/master/examples

You'll find sample scripts and workflows demonstrating how to use Earth Data Kit with different data sources and scenarios.

Environment Configuration
-------------------------
Earth Data Kit can be customized via environment variables, which you should define in your `.env` file. This lets you easily configure settings such as AWS credentials, GDAL options, and other operational parameters.

General Options
~~~~~~~~~~~~~~~
* ``DATA_DIR`` *(Required)*: The directory path used for storing and sharing data within the container (e.g., catalog, pre-processed VRTs). Is also used to create any intermediate files.
* ``WORKSPACE_DIR`` *(Required)*: The directory path used for storing your scripts, notebooks, etc.
* ``EDK_MAX_WORKERS``: The maximum number of workers to use for parallel processing. If not set, it will use ``num_cores - 2`` for CPU intensive tasks and ``(2 * num_cores) - 1`` for I/O intensive tasks.

AWS Options
~~~~~~~~~~~
* ``AWS_CONFIG_DIR``: By default, EDK uses `~/.aws` for AWS credentials and config. Set this variable to override the location.
* ``AWS_REGION``: AWS region where your data is stored (e.g., us-west-2). Use this when accessing S3.
* ``AWS_NO_SIGN_REQUEST`` (YES/NO): If set to YES, this option disables request signing, meaning AWS credentials will be bypassed.
* ``AWS_REQUEST_PAYER`` (requester): Indicates that the requester accepts any charges that may result from the request. Use this when accessing buckets that require payer confirmation.

Google Earth Engine Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~
* ``GOOGLE_APPLICATION_CREDENTIALS``: Specifies the path to the JSON credentials file for authenticating with the Earth Engine API. See the `Earth Engine service account guide <https://developers.google.com/earth-engine/guides/service_account>`_ for more information.

This configuration setup provides flexibility to adapt Earth Data Kit to your specific environment and processing needs.
