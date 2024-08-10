import spacetime_tools
import re
import datetime

# Derived from path of a single scene file
source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF"
ds = spacetime_tools.DataSet("modis-pds", source, "s3")

# Modis Data is at a daily frequency so we create one COG per day
destination = "/<local_path>/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"

# As an example we will use Albania's bounding box and get data for the month of January 2017 from s3://modis-pds/MCD43A4.006/
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)
date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 31)) # (Start, End) - End date is inclusive

# Setting time bounds
ds.set_timebounds(date_range[0], date_range[1])

# To get the h and v components from file path we need to define a 
# lambda function which can extract h and v from the grid dataframe as a dictionary
# Example below
def fn(df_row):
    match = re.search(r"h:(\d*) v:(\d*)", df_row.Name)
    if match and match.groups():
        vars = match.groups()
        return {
            "h": f"{int(vars[0]):02d}",
            "v": f"{int(vars[1]):02d}",
        }

# Setting spatial bounds. We pass the bbox we are interested in,
# grid file path and the lambda function to extract h and v from the grid file
ds.set_spacebounds(bbox, grid_fp, fn)

# Getting distinct bands. This will help us decide band arrangement when stitching scenes together
bands = ds.get_distinct_bands()
print (bands)

# Downloading scene files
ds.sync()

# Finally stitching them together with the band arrangement as below
ds.to_cog(
    destination,
    bands=[
        "Nadir_Reflectance_Band7",
    ],
)