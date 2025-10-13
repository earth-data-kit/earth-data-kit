import earth_data_kit as edk
from fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare
import os
import pytest
import tarfile

FIXTURES_DIR = "/app/workspace/fixtures"

def _run():
    source = "ECMWF/ERA5_LAND/MONTHLY_AGGR"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 2, 1))

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "surface-temp",
        source,
        "earth_engine",
        clean=True,
        format="earth_engine"
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    ds.discover()
    ds.mosaic(bands=["temperature_2m"])

    ds.save()

def _generated_golden_archives():
    # Create a tar file
    with tarfile.open(
        "/app/workspace/fixtures/goldens/gee-base-case.tar",
        "w:tar"
    ) as tar:
        tar.add(
            "/app/data/tmp/surface-temp/pre-processing",
            arcname="gee-base-case",
        )


def _test():
    output_base_vrt = f"/app/data/tmp/surface-temp/pre-processing"

    output_vrts = [f"{output_base_vrt}/2017-01-01-00:00:00.vrt"]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/goldens/gee-base-case.tar/gee-base-case/2017-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0


@pytest.mark.order(0)
def test_base_case():
    _run()

    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()

    _test()
