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


def test_base_case():
    source = "ECMWF/ERA5_LAND/MONTHLY_AGGR"
    destination = f"{OUTPUT_BASE_PATH}/surface-temp/%d-%m-%Y.tif"

    bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)
    xmin, ymin, xmax, ymax = bbox
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = DataSet(
        "surface-temp",
        source,
        "earth_engine",
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox)

    # Getting distinct bands
    bands = ds.get_distinct_bands()

    # Getting distinct bands
    bands = ds.get_distinct_bands()

    # Syncing data
    ds.sync()

    # Stitching data together as COGs
    ds.to_cog(
        destination,
        bands=["air"],
    )

    golden_files = [
        f"tests/fixtures/outputs/stitching/earth_engine/02-01-2017.tif",
    ]
    new_files = [
        f"{OUTPUT_BASE_PATH}/surface-temp/02-01-2017.tif",
    ]

    for i in range(len(golden_files)):
        assert (
            gdalcompare.compare_db(gdal.Open(golden_files[i]), gdal.Open(new_files[i]))
            == 0
        )
