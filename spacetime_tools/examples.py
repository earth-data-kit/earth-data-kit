import datetime
from spacetime_tools.stitching.sample_data.country_bboxes import country_bounding_boxes
import re
import pandas as pd
import logging
from dotenv import load_dotenv
import os

# from spacetime_tools.stitching.sync_and_stitch import sync_and_stitch
from spacetime_tools.stitching.classes import dataset

load_dotenv()
logger = logging.getLogger(__name__)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "x": f"{int(vars[0]):02d}",
            "y": f"{int(vars[1]):02d}",
        }


if __name__ == "__main__":
    source = "s3://modis-pds/MCD43A4.006/{x}/{y}/%Y%j/*_B0?.TIF"
    destination = "/Volumes/Data/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"
    grid_fp = "stitching/sample_data/sample_kmls/modis.kml"
    region = "us-west-2"
    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 10))

    # Creating a dataset
    ds = dataset.DataSet(
        "modis-pds",
        source,
        "s3",
        engine_opts={
            "no_sign_request": True,
        },
    )

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox[1], grid_fp, fn)

    # Getting distinct bands
    bands = ds.get_distinct_bands()
    print(bands)
    # # Syncing data
    ds.sync()

    # # Stitching data together as COGs
    ds.to_cog(
        destination,
        bands=[
            "Nadir_Reflectance_Band1",
            "Nadir_Reflectance_Band3",
            "Nadir_Reflectance_Band7",
        ],
    )
