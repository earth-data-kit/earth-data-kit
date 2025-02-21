import earth_data_kit as edk
import datetime

# Define the source path for MODIS scene files using a wildcard pattern.
# The pattern uses strftime formatting (%Y%j) to dynamically match dates within the S3 directory structure.
source = "s3://modis-pds/MCD43A4.006/*/*/%Y%j/*_B07.TIF"

# Instantiate the Dataset object for the MODIS collection using the S3 engine.
# The Dataset class handles scanning and filtering of scene files.
ds = edk.stitching.Dataset("modis-pds", source, "s3")


# Define the spatial extent (in EPSG:4326) using Albania's bounding box.
# Format: (min_longitude, min_latitude, max_longitude, max_latitude)
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)

# Define the temporal range for data selection.
# The dataset covers the month of January 2017; note that the end date is inclusive.
date_range = (
    datetime.datetime(2017, 1, 1),
    datetime.datetime(2017, 1, 31),
)

# Configure the Dataset with the defined time bounds.
ds.set_timebounds(date_range[0], date_range[1])

# Configure the spatial filter for the Dataset.
# Since no grid file is provided, the filtering is based solely on the bounding box.
ds.set_spacebounds(bbox)

# Discover the available scene files that match the defined criteria.
# This step scans the remote source and identifies all files that meet the spatial and temporal constraints.
ds.discover()

# Generate VRTs files for the discovered scene files.
ds.to_vrts(bands=["Nadir_Reflectance_Band7"])
