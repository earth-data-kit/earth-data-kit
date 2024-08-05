from spacetime_tools.stitching.classes.dataset import DataSet
import glob
import os
import re
import pandas as pd
from tests.fixtures.country_bboxes import country_bounding_boxes
import geopandas as gpd
import datetime
from dotenv import load_dotenv
from osgeo import gdal

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "h": f"{int(vars[0]):02d}",
            "v": f"{int(vars[1]):02d}",
        }


def test_grid_file():
    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF"
    destination = "/Volumes/Data/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"
    grid_fp = "tests/fixtures/modis.kml"

    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 10))

    # Creating a dataset
    ds = DataSet(
        "modis-pds",
        source,
        "s3",
        engine_opts={
            "no_sign_request": True,
        },
        clean=True,
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1], grid_fp, fn)

    # Asserting length of patterns created when calling bound methods.
    # AL == Albania is covered by 2 grids 19/4 and 19/5 according to modis grid
    assert len(ds.patterns) == (10 * 2)

    # Getting distinct bands
    bands = ds.get_distinct_bands()
    df = pd.DataFrame([[1, "Nadir_Reflectance_Band7", 3]], columns=["band_idx", "description", "dtype"])

    pd.testing.assert_frame_equal(bands, df)

    # Syncing data
    ds.sync()

    # Stitching data together as COGs
    ds.to_cog(
        destination,
        bands=[
            "Nadir_Reflectance_Band7",
        ],
    )

    out_ds = gdal.Open(
        "/Volumes/Data/spacetime-tools/final/modis-pds/01-01-2017-b07.TIF"
    )

    # Asserting number of files created
    out_files = glob.glob("/Volumes/Data/spacetime-tools/final/modis-pds/*.TIF")
    assert len(out_files) == 10

    # Asserting all file sizes to be greater than 10 MB
    for f in out_files:
        stats = os.stat(f)
        assert (stats.st_size/(1024 * 1024)) > 10

    # Asserting projection and resolution
    out_geo_transform = (
        1111950.519667,
        463.3127165274999,
        0.0,
        5559752.598333,
        0.0,
        -463.31271652750013,
    )
    out_projection = 'PROJCS["unnamed",GEOGCS["Unknown datum based upon the custom spheroid",DATUM["Not specified (based on custom spheroid)",SPHEROID["Custom spheroid",6371007.181,0]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Sinusoidal"],PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'

    assert out_ds.GetGeoTransform() == out_geo_transform
    assert out_ds.GetProjection() == out_projection
