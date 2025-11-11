import earth_data_kit as edk
from osgeo_utils import gdalcompare
import os
from osgeo import gdal
import pytest
import tarfile

FIXTURES_DIR = "/app/workspace/fixtures"


def _run():
    source = "AU/GA/AUSTRALIA_5M_DEM"

    # Small area in New Zealand
    bbox = (141.667156, -25.115290, 142.348309, -24.156622)

    # Creating a dataset
    ds = edk.stitching.Dataset(
        "nz-dem-5m",
        source,
        "earth_engine",
        clean=True,
        format="earth_engine",
    )

    # Setting spatial extent
    ds.set_spacebounds(bbox)

    ds.discover()

    print(ds.get_bands())
    ds.mosaic(bands=["elevation"], resolution=(5, -5), crs="EPSG:3857")
    ds.save()


def _test():
    output_base_vrt = f"/app/data/tmp/nz-dem-5m/pre-processing"

    output_vrts = [f"{output_base_vrt}/1970-01-01-00:00:00.vrt"]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/goldens/gee-no-time-dim.tar/gee-no-time-dim/1970-01-01-00:00:00.vrt",
    ]

    # helpers.compare_tar_dir(f"{FIXTURES_DIR}/goldens/gee-no-time-dim.tar", "/app/data/tmp/nz-dem-5m/pre-processing")
    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0


def _generated_golden_archives():
    # Create a tar file
    with tarfile.open(
        "/app/workspace/fixtures/goldens/gee-no-time-dim.tar", "w:tar"
    ) as tar:
        tar.add(
            "/app/data/tmp/nz-dem-5m/pre-processing",
            arcname="gee-no-time-dim",
        )


@pytest.mark.order(0)
def test_no_time_dim():
    _run()

    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()

    _test()
