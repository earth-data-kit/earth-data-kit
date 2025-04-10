import earth_data_kit as edk
from osgeo_utils import gdalcompare
from dotenv import load_dotenv
import os
from osgeo import gdal
import pytest
CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)
FIXTURES_DIR = "tests/fixtures"


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
    )

    # Setting spatial extent
    ds.set_spacebounds(bbox)

    ds.discover()
    ds.to_vrts(bands=["elevation"])


def _test():
    output_base_vrt = f"{os.getenv('TMP_DIR')}/tmp/nz-dem-5m/pre-processing"

    output_vrts = [f"{output_base_vrt}/1970-01-01-00:00:00.vrt"]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/earth_engine/no-time-dim.tar.gz/1970-01-01-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0

@pytest.mark.order(0)
def test_no_time_dim():
    _run()
    _test()
