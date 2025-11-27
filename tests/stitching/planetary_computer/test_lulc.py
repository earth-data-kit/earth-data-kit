"""
Planetary Computer Test - LULC (Impact Observatory Land Use Land Cover)
Tests EDK with the io-lulc-9-class collection (timeless)
"""

import earth_data_kit as edk
import os
from osgeo import gdal
import pytest
import tarfile
from pathlib import Path

# Dynamically determine paths relative to this test file
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
FIXTURES_DIR = str(PROJECT_ROOT / "tests" / "fixtures")
DEFAULT_TMP_DIR = str(PROJECT_ROOT / "data" / "tmp")

TEST_CONFIG = {
    "collection_id": "io-lulc-9-class",
    "bands": ["data"],
    "bbox": (77.0, 28.5, 77.5, 28.9),  # Example: New Delhi area
    "dataset_name": "pc-lulc-test",
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
    tmp_dir = os.getenv('TMP_DIR', DEFAULT_TMP_DIR)
    output_base_vrt = f"{tmp_dir}/{TEST_CONFIG['dataset_name']}/pre-processing"

    output_vrts = sorted([f for f in glob.glob(f"{output_base_vrt}/*.vrt")
                          if not any(x in f for x in ['-data.', '.tmp.'])])

    assert len(output_vrts) > 0, f"No VRT files found in {output_base_vrt}"

    golden_tar_path = f"{FIXTURES_DIR}/goldens/pc-lulc.tar"
    golden_base = f"/vsitar/{golden_tar_path}/pc-lulc"

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
    tmp_dir = os.getenv('TMP_DIR', DEFAULT_TMP_DIR)
    with tarfile.open(
        f"{FIXTURES_DIR}/goldens/pc-lulc.tar", "w:tar"
    ) as tar:
        tar.add(
            f"{tmp_dir}/{TEST_CONFIG['dataset_name']}/pre-processing",
            arcname="pc-lulc",
        )

@pytest.mark.order(2)
def test_lulc():
    _run()
    if os.getenv("GENERATE_GOLDEN_ARCHIVES") == "TRUE":
        _generated_golden_archives()
    _test()
