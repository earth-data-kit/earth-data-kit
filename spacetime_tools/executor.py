from spacetime_tools.stitching import sync
import datetime
from spacetime_tools.stitching.sample_data.country_bboxes import country_bounding_boxes

if __name__ == "__main__":
    pattern = "s3://modis-pds/MCD43A4.006/00/08/!YYYY!DDD/*_B07.TIF"

    bbox = country_bounding_boxes["IN"]
    date_range = (datetime.datetime(2014, 2, 1), datetime.datetime(2014, 4, 1))

    sync.sync(pattern=pattern, date_range=date_range)
