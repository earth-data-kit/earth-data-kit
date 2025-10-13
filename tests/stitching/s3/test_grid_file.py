from fiona.drvsupport import supported_drivers

supported_drivers["LIBKML"] = "rw"

import os
from fixtures.country_bboxes import country_bounding_boxes
import datetime
from osgeo import gdal
from osgeo_utils import gdalcompare
import earth_data_kit as edk
import pytest
import geopandas as gpd
import tarfile

FIXTURES_DIR = "/app/workspace/fixtures"

def _run():
    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF"
    grid_fp = "fixtures/modis.kml"

    gdf = gpd.read_file(grid_fp)
    gdf["h"] = (
        gdf["Name"]
        .str.split(" ")
        .str[0]
        .str.split(":")
        .str[1]
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )
    gdf["v"] = (
        gdf["Name"]
        .str.split(" ")
        .str[1]
        .str.split(":")
        .str[1]
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "modis-pds",
        source,
        "s3",
        clean=True,
        format="geotiff",
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1], gdf)

    # Discover catalogue
    ds.discover()

    print (ds.get_bands())

    # Stitching data together as VRTs
    ds.mosaic(bands=["Nadir_Reflectance_Band3", "Nadir_Reflectance_Band7"])

    ds.save()


def _test():
    output_base_vrt = f"/app/data/tmp/modis-pds/pre-processing"

    output_vrts = [
        f"{output_base_vrt}/2017-01-01-00:00:00.vrt",
    ]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/goldens/s3-grid-file.tar/s3-grid-file/2017-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0

def _generated_golden_archives():
    # Create a tar file
    with tarfile.open(
        "/app/workspace/fixtures/goldens/s3-grid-file.tar",
        "w:tar"
    ) as tar:
        tar.add(
            "/app/data/tmp/modis-pds/pre-processing",
            arcname="s3-grid-file",
        )

@pytest.mark.order(0)
def test_grid_file():
    os.environ["AWS_REGION"] = "us-west-2"
    os.environ["AWS_NO_SIGN_REQUEST"] = "YES"

    _run()

    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()
    _test()


test_grid_file()