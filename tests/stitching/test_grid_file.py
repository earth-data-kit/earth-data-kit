from spacetime_tools.stitching.classes.dataset import DataSet
import re
from tests.fixtures.country_bboxes import country_bounding_boxes
import geopandas as gpd
import datetime
from dotenv import load_dotenv
import os

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "x": f"{int(vars[0]):02d}",
            "y": f"{int(vars[1]):02d}",
        }


def test_grid_file():
    source = "s3://modis-pds/MCD43A4.006/{x}/{y}/%Y%j/*_B0?.TIF"
    destination = "/Volumes/Data/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"

    grid_fp = "tests/fixtures/modis.kml"
    gdf = gpd.read_file(grid_fp)

    print(gdf)
    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))

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

    print(ds.patterns)
    # # # Getting distinct bands
    # # bands = ds.get_distinct_bands()
    # # print(bands)
    # # # Syncing data
    # # ds.sync()

    # # # # Stitching data together as COGs
    # # ds.to_cog(
    # #     destination,
    # #     bands=[
    # #         "Nadir_Reflectance_Band1",
    # #         "Nadir_Reflectance_Band3",
    # #         "Nadir_Reflectance_Band7",
    # #     ],
    # # )

    assert False
