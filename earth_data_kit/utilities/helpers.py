# TODO: move to it's own folder, utilities/helpers.py
import os
import sys
import shutil
import hashlib
import logging
import json
import pandas as pd
import pathlib

logger = logging.getLogger(__name__)


def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
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


def get_tmp_dir():
    tmp_dir = f'{os.getenv("TMP_DIR")}/tmp'
    make_sure_dir_exists(tmp_dir)
    return tmp_dir


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
