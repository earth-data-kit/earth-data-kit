# TODO: move to it's own folder, utilities/helpers.py
import os
import sys
import shutil
import hashlib
import logging
import json
import pandas as pd
import pathlib
import numpy as np

logger = logging.getLogger(__name__)


def has_wildcards(path):
    """
    Check if a path contains wildcard characters (* or ?).

    Args:
        path (str): The path to check for wildcards.

    Returns:
        bool: True if the path contains wildcards, False otherwise.

    Raises:
        ValueError: If the path contains '**' which is not supported.
    """
    if "**" in path:
        raise ValueError("Double asterisk '**' wildcard is not supported")

    return "*" in path or "?" in path


def make_sure_dir_exists(dir):
    # Check if the path is a file path (has an extension)
    if os.path.splitext(dir)[1]:
        # Extract the directory part of the file path
        dir = os.path.dirname(dir)

    # Create the directory if it doesn't exist
    if dir and not os.path.exists(dir):
        os.makedirs(dir)


def get_processpool_workers():
    try:
        if os.getenv("EDK_MAX_WORKERS"):
            return int(os.getenv("EDK_MAX_WORKERS"))
    except Exception as e:
        logger.warning(
            f"Error getting EDK_MAX_WORKERS: {e}. Returning default value using max(1,os.cpu_count() - 2)"
        )
        return max(1, os.cpu_count() - 2)  # type: ignore


def get_threadpool_workers():
    try:
        if os.getenv("EDK_MAX_WORKERS"):
            return int(os.getenv("EDK_MAX_WORKERS"))
    except Exception as e:
        logger.warning(
            f"Error getting EDK_MAX_WORKERS: {e}. Returning default value using max(1,2*os.cpu_count() - 1)"
        )
        return max(1, 2 * os.cpu_count() - 1)  # type: ignore


def remove_file_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    return False


def get_tmp_dir():
    # Giving path inside container
    if is_running_in_docker():
        tmp_dir = f"/app/data/tmp"
    else:
        tmp_dir = f'{os.getenv("DATA_DIR")}/tmp'
    make_sure_dir_exists(tmp_dir)
    return tmp_dir


def is_running_in_docker():
    """
    Returns True if the current process is running inside a Docker container.
    """
    # Check for the existence of /.dockerenv
    if os.path.exists("/.dockerenv"):
        return True
    # Check cgroup info for docker/lxc keywords
    try:
        with open("/proc/1/cgroup", "rt") as f:
            content = f.read()
            if (
                "docker" in content
                or "kubepods" in content
                or "containerd" in content
                or "lxc" in content
            ):
                return True
    except Exception:
        pass
    return False


def delete_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)


def cheap_hash(input):
    return hashlib.md5(input.encode("utf-8")).hexdigest()[:6]


def json_to_series(text):
    keys, values = zip(*[item for dct in json.loads(text) for item in dct.items()])
    return pd.Series(values, index=keys)


def get_platform():
    if sys.platform == "darwin":
        return "macos"
    elif sys.platform == "linux":
        return "linux"
    else:
        raise Exception(f"Unsupported platform: {sys.platform}")


def get_shared_lib_path():
    path = os.path.join(
        pathlib.Path(__file__).parent.parent.resolve(),
        "stitching",
        "shared_libs",
        "builds",
    )
    if get_platform() == "macos":
        arch = "go-lib-darwin-arm64"
    elif get_platform() == "linux":
        arch = "go-lib-linux-amd64"

    if arch == None:
        raise Exception(f"Unsupported platform: {sys.platform}")

    path = os.path.join(path, arch)

    return path


def scale_to_255(arr):
    np.nanmax(arr)
    np.nanmin(arr)

    # Scale values between 0-255
    # First handle NaN values and identify min/max
    arr_no_nan = arr[~np.isnan(arr)]
    arr_min = np.min(arr_no_nan)
    arr_max = np.max(arr_no_nan)

    # Create a copy to avoid modifying the original array
    arr_scaled = arr.copy()

    # Scale the non-NaN values between 0-255
    if arr_max > arr_min:  # Avoid division by zero if all values are the same
        arr_scaled[~np.isnan(arr)] = (
            (arr_no_nan - arr_min) / (arr_max - arr_min)
        ) * 255

    # TODO: We need a way to handle the NaN values, are not able to convert to unit8
    # # Convert to uint8 for proper image representation
    # arr_scaled = arr_scaled.astype(np.uint8)

    return arr_scaled
