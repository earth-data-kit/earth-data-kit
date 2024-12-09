from earth_data_kit.stitching.classes.dataset import DataSet
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

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)


def test_wo_grid_file():
    source = "s3://modis-pds/MCD43A4.006/*/*/%Y%j/*_B07.TIF"
    destination = f"{OUTPUT_BASE_PATH}/modis-pds/%d-%m.tif"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = DataSet(
        "modis-pds",
        source,
        "s3",
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    # Getting distinct bands
    bands = ds.get_distinct_bands()

    # Syncing data
    ds.sync()

    # Stitching data together as COGs
    ds.to_cog(
        destination,
        bands=[
            "Nadir_Reflectance_Band7",
        ],
    )

    golden_files = [
        f"tests/fixtures/outputs/stitching/s3/wo_grid_file/01-01.tif",
        f"tests/fixtures/outputs/stitching/s3/wo_grid_file/02-01.tif",
    ]
    new_files = [
        f"{OUTPUT_BASE_PATH}/modis-pds/01-01.tif",
        f"{OUTPUT_BASE_PATH}/modis-pds/02-01.tif",
    ]

    for i in range(len(golden_files)):
        assert (
            gdalcompare.compare_db(gdal.Open(golden_files[i]), gdal.Open(new_files[i]))
            == 0
        )
