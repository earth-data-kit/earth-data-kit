import os
import re
from tests.fixtures.country_bboxes import country_bounding_boxes
import datetime
from dotenv import load_dotenv
from osgeo import gdal
from osgeo_utils import gdalcompare
import earth_data_kit as edk
import random
from osgeo import osr

CONFIG_FILE_PATH = "tests/config.env"
FIXTURES_DIR = "tests/fixtures"
load_dotenv(CONFIG_FILE_PATH)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "h": f"{int(vars[0]):02d}",
            "v": f"{int(vars[1]):02d}",
        }


def get_raster_extent(raster_path):
    dataset = gdal.Open(raster_path)
    if not dataset:
        raise FileNotFoundError(f"Unable to open {raster_path}")

    geotransform = dataset.GetGeoTransform()
    if not geotransform:
        raise ValueError(f"Unable to get geotransform from {raster_path}")

    x_min = geotransform[0]
    y_max = geotransform[3]
    x_max = x_min + geotransform[1] * dataset.RasterXSize
    y_min = y_max + geotransform[5] * dataset.RasterYSize

    return x_min, y_min, x_max, y_max

def get_random_lat_lon_within_extent(raster_path, num_points=1):
    x_min, y_min, x_max, y_max = get_raster_extent(raster_path)
    points = []
    for _ in range(num_points):
        x = random.uniform(x_min, x_max)
        y = random.uniform(y_min, y_max)
        points.append((x, y))
    return points

def convert_to_lat_lon(raster_path, points):
    dataset = gdal.Open(raster_path)
    if not dataset:
        raise FileNotFoundError(f"Unable to open {raster_path}")

    source_proj = osr.SpatialReference()
    source_proj.ImportFromWkt(dataset.GetProjection())
    target_proj = osr.SpatialReference()
    target_proj.ImportFromEPSG(4326)  # WGS84

    transform = osr.CoordinateTransformation(source_proj, target_proj)
    lat_lon_points = [transform.TransformPoint(x, y)[:2] for x, y in points]
    return lat_lon_points

def run():
    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF"
    grid_fp = "tests/fixtures/modis.kml"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

    # Creating a dataset
    ds = edk.Dataset(
        "modis-pds",
        source,
        "s3",
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1], grid_fp, fn)

    # Discover catalogue
    ds.discover()
    
    # Stitching data together as VRTs
    ds.to_vrts(
        bands=["Nadir_Reflectance_Band3", "Nadir_Reflectance_Band7"]
    )

def test():
    output_base_vrt = f"{os.getenv('TMP_DIR')}/tmp/modis-pds/pre-processing"

    output_vrts = [f"{output_base_vrt}/2017-01-01-00:00:00.vrt", f"{output_base_vrt}/2017-01-02-00:00:00.vrt"]
    golden_files = [
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/s3/with-grid-file.tar.gz/2017-01-01-00:00:00.vrt",
        f"/vsitar/{FIXTURES_DIR}/outputs/stitching/s3/with-grid-file.tar.gz/2017-01-02-00:00:00.vrt",
    ]

    for output_vrt, golden_file in zip(output_vrts, golden_files):
        print (f"Comparing {output_vrt} with {golden_file}")
        ds = gdal.Open(output_vrt)
        ds_golden = gdal.Open(golden_file)

        assert gdalcompare.compare_db(ds_golden, ds) == 0

def test_grid_file():
    run()
    test()

if __name__ == "__main__":
    test_grid_file()