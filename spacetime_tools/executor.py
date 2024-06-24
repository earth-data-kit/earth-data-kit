from spacetime_tools.stitching import sync
import datetime
from spacetime_tools.stitching.sample_data.country_bboxes import country_bounding_boxes
import re
import logging

logger = logging.getLogger(__name__)

def fn(x):
    match = re.search(r'h:(\d*) v:(\d*)', x.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "horizontal_grid": vars[0],
            "vertical_grid": vars[1]
        }

if __name__ == "__main__":
    pattern = "s3://modis-pds/MCD43A4.006/!horizontal_grid!/!vertical_grid!/!YYYY!!DDD!/*_B07.TIF"
    grid_fp = "stitching/sample_data/sample_kmls/modis.kml"

    bbox = country_bounding_boxes["IN"]
    date_range = (datetime.datetime(2010, 2, 1), datetime.datetime(2013, 4, 1))

    sync.sync(pattern=pattern, date_range=date_range, grid_fp=grid_fp, matcher=fn)
