from spacetime_tools.stitching import sync, discover
import datetime
from spacetime_tools.stitching.sample_data.country_bboxes import country_bounding_boxes
import re
import logging
from dotenv import load_dotenv
import os
from spacetime_tools.stitching.helpers import get_tmp_dir
load_dotenv()
logger = logging.getLogger(__name__)


def fn(x):
    match = re.search(r"h:(\d*) v:(\d*)", x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "horizontal_grid": f"{int(vars[0]):02d}",
            "vertical_grid": f"{int(vars[1]):02d}",
        }


if __name__ == "__main__":
    pattern = "s3://modis-pds/MCD43A4.006/!horizontal_grid!/!vertical_grid!/!YYYY!!DDD!/*_B07.TIF"
    grid_fp = "stitching/sample_data/sample_kmls/modis.kml"
    region = "us-west-2"
    bbox = country_bounding_boxes["IN"]
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 12, 31))

    fp = "/Volumes/Data/spacetime-tools/tmp/raw/2017*/*_B07.TIF"
    discover.discover(fp)


    # sync.sync(
    #     pattern=pattern,
    #     date_range=date_range,
    #     grid_fp=grid_fp,
    #     matcher=fn,
    #     bbox=bbox[1],
    #     engine_opts={
    #         "region": region,
    #     },
    # )
