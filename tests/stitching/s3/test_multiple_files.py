import earth_data_kit as edk
import os
from fixtures.country_bboxes import country_bounding_boxes
from osgeo import gdal
from osgeo_utils import gdalcompare
import pytest
import tarfile

FIXTURES_DIR = "/app/workspace/fixtures"


def _test():
    output_base_vrt = f"/app/data/tmp/global-land-cover/pre-processing"

    output_vrts = [
        f"{output_base_vrt}/1970-01-01-00:00:00.vrt",
    ]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/goldens/s3-multiple-files.tar/s3-multiple-files/1970-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0


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
        "global-land-cover",
        source,
        "s3",
        clean=True,
        format="geotiff",
    )

    # Setting spatial extent
    ds.set_spacebounds(bbox[1])

    # Discover catalogue
    ds.discover()

    # Stitching data together as VRTs
    ds.mosaic(
        bands=["NoDescription"],
        crs="EPSG:4326",
        resolution=(0.0042, -0.0042),
    )

    ds.save()


def _generated_golden_archives():
    # Create a tar file
    with tarfile.open(
        "/app/workspace/fixtures/goldens/s3-multiple-files.tar", "w:tar"
    ) as tar:
        tar.add(
            "/app/data/tmp/global-land-cover/pre-processing",
            arcname="s3-multiple-files",
        )


@pytest.mark.order(1)
def test_multiple_files():
    # Setting the region to ap-south-1 as we are using test-land-cover bucket which is in ap-south-1
    os.environ["AWS_REGION"] = "ap-south-1"
    os.environ["AWS_NO_SIGN_REQUEST"] = "NO"

    _run()

    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()

    _test()
