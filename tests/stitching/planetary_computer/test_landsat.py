
"""
Planetary Computer Test - Landsat
Tests EDK with Landsat Collection 2 Level-2 data
"""

import earth_data_kit as edk
import os
import datetime
from osgeo import gdal
import pytest
import tarfile

FIXTURES_DIR = "/app/workspace/fixtures"

TEST_CONFIG = {
    "collection_id": "landsat-c2-l2",
    "bands": ["red", "green", "blue"],  # Red, Green, Blue for Landsat
    "date_range": (datetime.datetime(2024, 12, 26), datetime.datetime(2024, 12, 27)),
    "bbox": (-122.45, 37.75, -122.40, 37.80),  # Small bbox in San Francisco
    "dataset_name": "pc-landsat-test",
}


def _run():
    collection_id = TEST_CONFIG["collection_id"]
    source = f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection_id}"

    ds = edk.stitching.Dataset(
        TEST_CONFIG["dataset_name"],
        source,
        "planetary_computer",
        clean=True,
        format="stac_asset",
    )

    ds.set_timebounds(TEST_CONFIG["date_range"][0], TEST_CONFIG["date_range"][1])
    ds.set_spacebounds(TEST_CONFIG["bbox"])
    ds.discover()

    ds.mosaic(
        bands=TEST_CONFIG["bands"],
        sync=False,
        overwrite=True,
    )

    ds.save()


def _test():
    import glob
    tmp_dir = os.getenv('TMP_DIR', '/app/data/tmp')
    output_base_vrt = f"{tmp_dir}/{TEST_CONFIG['dataset_name']}/pre-processing"

    output_vrts = sorted([f for f in glob.glob(f"{output_base_vrt}/*.vrt")
                          if not any(x in f for x in ['-red.', '-green.', '-blue.', '.tmp.'])])

    assert len(output_vrts) > 0, f"No VRT files found in {output_base_vrt}"

    golden_base = f"/vsitar/{FIXTURES_DIR}/goldens/pc-landsat.tar/pc-landsat"

    for output_vrt in output_vrts:
        vrt_filename = os.path.basename(output_vrt)
        golden_file = f"{golden_base}/{vrt_filename}"

        print(f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert ds is not None, f"Failed to open {output_vrt}"
        assert ds_golden is not None, f"Failed to open golden file {golden_file}"

        assert ds.RasterXSize == ds_golden.RasterXSize, "Raster width mismatch"
        assert ds.RasterYSize == ds_golden.RasterYSize, "Raster height mismatch"
        assert ds.RasterCount == ds_golden.RasterCount, "Band count mismatch"


def _generated_golden_archives():
    tmp_dir = os.getenv('TMP_DIR', '/app/data/tmp')
    with tarfile.open(
        f"{FIXTURES_DIR}/goldens/pc-landsat.tar", "w:tar"
    ) as tar:
        tar.add(
            f"{tmp_dir}/{TEST_CONFIG['dataset_name']}/pre-processing",
            arcname="pc-landsat",
        )


@pytest.mark.order(0)
def test_landsat():
    _run()
    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()
    _test()
