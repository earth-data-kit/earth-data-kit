import earth_data_kit as edk
from tests.fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare
import os

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)
FIXTURES_DIR = "tests/fixtures"

def run():
    source = "ECMWF/ERA5_LAND/MONTHLY_AGGR"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = edk.Dataset(
        "surface-temp",
        source,
        "earth_engine",
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    ds.discover()
    ds.to_vrts(bands=["temperature_2m"])

def test():
    output_base_vrt = f"{os.getenv('TMP_DIR')}/tmp/surface-temp/pre-processing"

    output_vrts = [f"{output_base_vrt}/2017-01-01-00:00:00.vrt"]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/earth_engine/base-case.tar.gz/2017-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print (f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0
