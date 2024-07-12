import datetime
from spacetime_tools.stitching.sample_data.country_bboxes import country_bounding_boxes
import re
import pandas as pd
import logging
from dotenv import load_dotenv
import os
from spacetime_tools.stitching.sync_and_stitch import sync_and_stitch

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
    pattern = "s3://modis-pds/MCD43A4.006/{x}/{y}/%Y%j/*_B07.TIF"
    output = "/Volumes/Data/spacetime-tools/tmp/modis-pds/%d-%m-%Y/{band}.TIF"
    grid_fp = "stitching/sample_data/sample_kmls/modis.kml"
    region = "us-west-2"
    bbox = country_bounding_boxes["AL"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 10))

    sync_and_stitch(
        engine="s3",
        source=pattern,
        bbox=bbox[1],
        date_range=date_range,
        grid_fp=grid_fp,
        matcher=fn,
        dest="/Volumes/Data/modis-pds/%d-%m-%Y.tif",
    )
