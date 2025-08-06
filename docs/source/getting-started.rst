Getting Started
===============

Primary Usage: Docker
---------------------
The recommended and primary way to run Earth Data Kit (EDK) is via Docker. This approach avoids dependency issues and ensures a consistent environment across platforms.

Quick Start
-----------
1. **Clone the repository:**

   .. code-block:: console

      $ git clone https://github.com/earth-data-kit/earth-data-kit.git
      $ cd earth-data-kit

2. **Create your `.env` file:**

   Set up your environment variables in a `.env` file in the root of the repository. See the "Environment Configuration" section below for details on available options.

3. **Initialize the Docker container:**

   .. code-block:: console

      $ bash init.sh

   This will build and start the EDK Docker container with all dependencies pre-installed.  
   Additionally, if you have a `requirements.txt` file inside the `workspace` directory, it will be installed automatically inside the container.

4. **Run your Python script inside the container:**

   Place your script (e.g., `my_script.py`) inside the `workspace` directory. Then run:

   .. code-block:: console

      $ bash launcher.sh my_script.py

   This will execute your script using Python 3 inside the running EDK container.

5. **(Optional) SSH into the container:**

   If you want an interactive shell inside the container, run:

   .. code-block:: console

      $ bash exec-edk.sh

   This will open a bash shell inside the EDK container, allowing you to run commands interactively.
   
6. **(Optional) Start a JupyterLab server inside the container:**

   If you want to use JupyterLab for interactive development, you can launch a JupyterLab server inside the EDK container by running:

   .. code-block:: console

      $ bash start-notebook.sh

   This will start a JupyterLab server accessible from your browser. By default, it will be available at `http://localhost:8888` on your host machine. You can then open notebooks and interact with your code and data directly within the container environment.

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
