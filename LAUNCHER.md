## EDK LAUNCHER:
Running and Initializing Your Workspace in Docker

This documentation explains how to use two scripts, `init.sh` and `launcher.sh`, to set up your development environment and run scripts inside a Docker container. These scripts are designed to make it easy to manage dependencies, mount your workspace, and execute code seamlessly.

---

## 1. `init.sh`: Initialize and Launch the Docker Container

The `init.sh` script will:

- Build and launch the Docker container.
- Mounts the following folders:
  - `workspace` (where your scripts are)
  - `data` (for datasets)
- Copies the `earth_data_kit` source code into the container, builds and installs the package.
- Install any Python dependencies listed in your `requirements.txt` (if present in your workspace).
- Installation happens inside a virtual env inside the container. Path to env: `/opt/venv`
- Loads environment variables from a `.env` file at the project root.

## 2. `launcher.sh`: Run Code Inside the Docker Container

The `launcher.sh` script is designed to execute any code or script located within the `workspace` directory of the Docker container. This means you can run your Python scripts, notebooks, or other executables from your local `workspace` folder as if you were working directly on your machine, but with all the dependencies and environment managed by Docker.

To use `launcher.sh`, simply provide the path to your script (relative to the `workspace` directory) as an argument. You can also pass additional arguments to your script as needed.

**Example usage:**

`bash launcher.sh /workspace/scripts/my_script.py arg1 arg2`

## 3. `start-notebook.sh`: Launch Jupyter Notebook in Docker

The `start-notebook.sh` script is a convenience tool to start a Jupyter Notebook server inside the Docker container, making it easy to interactively explore and analyze your data and code. When you run this script, it will:

- Start a Jupyter Notebook server inside the container, using the Python environment and dependencies already set up.
- Expose the notebook interface on port `8888`, so you can access it from your browser at [http://localhost:8888](http://localhost:8888).
- Use the `workspace` directory as the root for your notebooks, so you can create, edit, and run notebooks directly on your project files.

**Example usage:**

`bash start-notebook.sh`

After running the script, open your browser and go to [http://localhost:8888](http://localhost:8888) to access the Jupyter Notebook interface.