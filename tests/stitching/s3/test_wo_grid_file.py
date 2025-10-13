import earth_data_kit as edk
import os
import pytest
from fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare

FIXTURES_DIR = "tests/fixtures"

def _test():
    output_base_vrt = (
        f"{os.getenv('TMP_DIR')}/tmp/modis-pds-wo-grid-file/pre-processing"
    )

    output_vrts = [
        f"{output_base_vrt}/2017-01-01-00:00:00.vrt",
        f"{output_base_vrt}/2017-01-02-00:00:00.vrt",
    ]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/s3/without-grid-file.tar.gz/2017-01-01-00:00:00.vrt",
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/s3/without-grid-file.tar.gz/2017-01-02-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0


def _run():
    source = "s3://modis-pds/MCD43A4.006/*/*/%Y%j/*_B07.TIF"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "modis-pds-wo-grid-file",
        source,
        "s3",
        clean=True,
        format="geotiff",
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    # Discover catalogue
    ds.discover()

    # Stitching data together as VRTs
    ds.mosaic(
        bands=["Nadir_Reflectance_Band7"],
    )

    ds.save()


@pytest.mark.order(0)
def _test_wo_grid_file():
    # TODO: This is too slow. s5cmd tries to get all files till first wildcard and filter them in memory
    os.environ["AWS_REGION"] = "us-west-2"
    os.environ["AWS_NO_SIGN_REQUEST"] = "YES"

    _run()
    _test()
