from earth_data_kit.stitching.classes.dataset import Dataset
import glob
import os
import re
import pandas as pd
from tests.fixtures.country_bboxes import country_bounding_boxes
from tests.fixtures.constants import OUTPUT_BASE_PATH
import geopandas as gpd
import datetime
from dotenv import load_dotenv
from osgeo import gdal
import pathlib
from osgeo_utils import gdalcompare
import time
import random
from osgeo import osr

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "h": f"{int(vars[0]):02d}",
            "v": f"{int(vars[1]):02d}",
        }


def get_raster_extent(raster_path):
    dataset = gdal.Open(raster_path)
    if not dataset:
        raise FileNotFoundError(f"Unable to open {raster_path}")

    geotransform = dataset.GetGeoTransform()
    if not geotransform:
        raise ValueError(f"Unable to get geotransform from {raster_path}")

    x_min = geotransform[0]
    y_max = geotransform[3]
    x_max = x_min + geotransform[1] * dataset.RasterXSize
    y_min = y_max + geotransform[5] * dataset.RasterYSize

    return x_min, y_min, x_max, y_max

def get_random_lat_lon_within_extent(raster_path, num_points=1):
    x_min, y_min, x_max, y_max = get_raster_extent(raster_path)
    points = []
    for _ in range(num_points):
        x = random.uniform(x_min, x_max)
        y = random.uniform(y_min, y_max)
        points.append((x, y))
    return points

def convert_to_lat_lon(raster_path, points):
    dataset = gdal.Open(raster_path)
    if not dataset:
        raise FileNotFoundError(f"Unable to open {raster_path}")

    source_proj = osr.SpatialReference()
    source_proj.ImportFromWkt(dataset.GetProjection())
    target_proj = osr.SpatialReference()
    target_proj.ImportFromEPSG(4326)  # WGS84

    transform = osr.CoordinateTransformation(source_proj, target_proj)
    lat_lon_points = [transform.TransformPoint(x, y)[:2] for x, y in points]
    return lat_lon_points

# Example usage
# extent = get_raster_extent("path/to/your/raster/file.tif")
# print(extent)
# random_points = get_random_lat_lon_within_extent("path/to/your/raster/file.tif", 5)
# print(random_points)
# lat_lon_points = convert_to_lat_lon("path/to/your/raster/file.tif", random_points)
# print(lat_lon_points)


def test_grid_file():
    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF"
    destination = f"{OUTPUT_BASE_PATH}/modis-pds/%d-%m-%Y.vrt"
    grid_fp = "tests/fixtures/modis.kml"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = Dataset(
        "modis-pds",
        source,
        "s3",
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1], grid_fp, fn)

    # Discover catalogue
    ds.discover()

    print (ds.get_bands())
    
    # Stitching data together as VRTs
    ds.to_vrts(
        bands=["Nadir_Reflectance_Band7", "Nadir_Reflectance_Band3"]
    )
    assert False

    points = get_raster
    # golden_files = [
    #     f"tests/fixtures/outputs/stitching/s3/grid_file/01-01-2017.vrt",
    #     f"tests/fixtures/outputs/stitching/s3/grid_file/02-01-2017.vrt",
    # ]
    # new_files = [
    #     f"{OUTPUT_BASE_PATH}/modis-pds/01-01-2017.vrt",
    #     f"{OUTPUT_BASE_PATH}/modis-pds/02-01-2017.vrt",
    # ]
    # time.sleep(2)
    # for i in range(len(golden_files)):
    #     assert (
    #         gdalcompare.compare_db(gdal.Open(golden_files[i]), gdal.Open(new_files[i]))
    #         == 0
    #     )
