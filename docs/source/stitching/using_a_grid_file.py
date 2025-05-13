import earth_data_kit as edk
import datetime
import geopandas as gpd

# Define the source file pattern with placeholders for horizontal (h) and vertical (v)
# grid indices. These placeholders will be dynamically populated using the grid file.
source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF"

# Create a Dataset instance using the S3 engine.
# "modis-pds" serves as an identifier for the dataset.
ds = edk.stitching.Dataset("modis-pds", source, "s3")

# Specify the path to the grid file. This file (e.g., a KML) maps the provider's grid system
# to global coordinates, allowing targeted file selection.
grid_fp = "tests/fixtures/modis.kml"

# Define the spatial bounding box for Albania.
# Format: (min_longitude, min_latitude, max_longitude, max_latitude)
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)

# Define the temporal range for January 2017 (end date inclusive).
start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2017, 1, 31)
ds.set_timebounds(start_date, end_date)

# Read the grid file and extract the grid components creating a grid dataframe
gdf = gpd.read_file(grid_fp)
gdf["h"] = (
    gdf["Name"]
    .str.split(" ")
    .str[0]
    .str.split(":")
    .str[1]
    .astype(int)
    .astype(str)
    .str.zfill(2)
)
gdf["v"] = (
    gdf["Name"]
    .str.split(" ")
    .str[1]
    .str.split(":")
    .str[1]
    .astype(int)
    .astype(str)
    .str.zfill(2)
)

# Configure the spatial bounds with the bounding box and grid dataframe
ds.set_spacebounds(bbox, grid_dataframe=gdf)

# Runs the discovery process to find the scene files that match the specified spatial and temporal constraints.
ds.discover()

# Stitches the scene files into VRTs using the defined band arrangement.
ds.to_vrts(bands=["Nadir_Reflectance_Band7"])
