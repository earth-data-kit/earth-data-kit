# TODO: move to it's own folder, utilities/helpers.py
import os
import sys
import shutil
from shapely import Polygon
import hashlib
import logging
import json
import pandas as pd
import shapely
from osgeo import gdal
import pathlib

logger = logging.getLogger(__name__)


def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def get_processpool_workers():
    if os.cpu_count() - 2 < 1:  # type: ignore
        return 1
    else:
        return os.cpu_count() - 2  # type: ignore


def get_threadpool_workers():
    return (2 * os.cpu_count()) - 1  # type: ignore


def get_tmp_dir():
    tmp_dir = f'{os.getenv("TMP_DIR")}/tmp'
    make_sure_dir_exists(tmp_dir)
    return tmp_dir


def delete_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)


# TODO: move to geo.py
def warp_and_get_extent(df_row):
    ds = gdal.Warp(
        "/vsimem/reprojected.tif", gdal.Open(df_row.gdal_path), dstSRS="EPSG:4326"
    )
    geo_transform = ds.GetGeoTransform()
    x_min = geo_transform[0]
    y_max = geo_transform[3]
    x_max = x_min + geo_transform[1] * ds.RasterXSize
    y_min = y_max + geo_transform[5] * ds.RasterYSize
    polygon = shapely.geometry.box(x_min, y_min, x_max, y_max, ccw=True)
    ds = None
    return polygon


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
    if get_platform() == "macos":
        path = os.path.join(
            pathlib.Path(__file__).parent.resolve(),
            "shared_libs",
            "builds",
            "go-lib-darwin-arm64",
        )
    elif get_platform() == "linux":
        path = os.path.join(
            pathlib.Path(__file__).parent.resolve(),
            "shared_libs",
            "builds",
            "go-lib-linux-amd64",
        )
    else:
        raise Exception(f"Unsupported platform: {sys.platform}")
    return path
