import earth_data_kit as edk
import os
from tests.fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare
import pytest

CONFIG_FILE_PATH = "tests/config.env"
FIXTURES_DIR = "tests/fixtures"
load_dotenv(CONFIG_FILE_PATH)


def _test():
    output_base_vrt = f"{os.getenv('TMP_DIR')}/tmp/global-land-cover/pre-processing"

    output_vrts = [
        f"{output_base_vrt}/1970-01-01-00:00:00.vrt",
    ]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/s3/single-file.tar.gz/1970-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0


def _run():
    source = "s3://test-land-cover/gm_lc_v3_1_2.tif"

    bbox = country_bounding_boxes["IN"]

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "global-land-cover",
        source,
        "s3",
        clean=True,
    )

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    # Discover catalogue
    ds.discover()

    # Stitching data together as VRTs
    ds.to_vrts(
        bands=["NoDescription"],
    )


@pytest.mark.order(0)
def test_multiple_files():
    # Setting the region to ap-south-1 as we are using test-land-cover bucket which is in ap-south-1
    os.environ["AWS_REGION"] = "ap-south-1"
    os.environ["AWS_NO_SIGN_REQUEST"] = "NO"

    _run()
    _test()
