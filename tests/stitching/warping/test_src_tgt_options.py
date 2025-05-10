import os
import re
from tests.fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare
import earth_data_kit as edk
import pytest

CONFIG_FILE_PATH = "tests/config.env"
FIXTURES_DIR = "tests/fixtures"
load_dotenv(CONFIG_FILE_PATH)


def _run():
    source = [
        "s3://test-land-cover/gm_lc_v3_1_1.tif",
        "s3://test-land-cover/gm_lc_v3_1_2.tif",
        "s3://test-land-cover/gm_lc_v3_2_1.tif",
        "s3://test-land-cover/gm_lc_v3_2_2.tif",
    ]

    bbox = country_bounding_boxes["IN"]

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "global-land-cover-4326",
        source,
        "s3",
        clean=True,
    )

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    # Discover catalogue
    ds.discover()

    ds.set_src_options({"-srcnodata": "0"})
    ds.set_target_options({"-tr": "1 1", "-t_srs": "EPSG:4326"})

    # Stitching data together as VRTs
    ds.mosaic(
        bands=["NoDescription"],
    )


def _test():
    output_base_vrt = (
        f"{os.getenv('TMP_DIR')}/tmp/global-land-cover-4326/pre-processing"
    )

    output_vrt = f"{output_base_vrt}/1970-01-01-00:00:00.vrt"

    ds = gdal.Open(output_vrt)

    print(ds.GetGeoTransform())
    res = ds.GetGeoTransform()[1], ds.GetGeoTransform()[5]

    assert res == (1.0, -1.0)

    nodataval = ds.GetRasterBand(1).GetNoDataValue()
    assert nodataval == 0

    epsg = ds.GetSpatialRef().GetAuthorityCode(None)
    assert epsg == "4326"


@pytest.mark.order(0)
def test_src_tgt_options():
    # Setting the region to ap-south-1 as we are using test-land-cover bucket which is in ap-south-1
    os.environ["AWS_REGION"] = "ap-south-1"
    os.environ["AWS_NO_SIGN_REQUEST"] = "NO"

    _run()
    _test()
