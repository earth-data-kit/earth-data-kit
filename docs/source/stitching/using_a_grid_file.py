import earth_data_kit as edk
import datetime
import re

# Define the source file pattern with placeholders for horizontal (h) and vertical (v)
# grid indices. These placeholders will be dynamically populated using the grid file.
source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF"

# Create a Dataset instance using the S3 engine.
# "modis-pds" serves as an identifier for the dataset.
ds = edk.stitching.Dataset("modis-pds", source, "s3")

# Specify the path to the grid file. This file (e.g., a KML) maps the provider's grid system
# to global coordinates, allowing targeted file selection.
grid_file = "tests/fixtures/modis.kml"

# Define the spatial bounding box for Albania.
# Format: (min_longitude, min_latitude, max_longitude, max_latitude)
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)

# Define the temporal range for January 2017 (end date inclusive).
start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2017, 1, 31)
ds.set_timebounds(start_date, end_date)


# Define a function to extract grid components (horizontal 'h' and vertical 'v')
# from a row in the grid file. The function expects the row's 'Name' attribute to contain
# a string in the format "h:<number> v:<number>".
def extract_grid_components(row):
    """
    Extracts grid indices from a grid file row.

    Args:
        row: An object representing a row from the grid file, which must include a 'Name' attribute.

    Returns:
        dict: A dictionary with grid identifiers 'h' and 'v', formatted as two-digit strings.
              Example: {'h': '01', 'v': '08'}
    """
    match = re.search(r"h:(\d+)\s+v:(\d+)", row.Name)
    if match:
        return {
            "h": f"{int(match.group(1)):02d}",
            "v": f"{int(match.group(2)):02d}",
        }
    return {}


# Configure the spatial bounds with the bounding box, grid file, and grid extraction function.
ds.set_spacebounds(bbox, grid_file, extract_grid_components)

# Runs the discovery process to find the scene files that match the specified spatial and temporal constraints.
ds.discover()

# Stitches the scene files into VRTs using the defined band arrangement.
ds.to_vrts(bands=["Nadir_Reflectance_Band7"])
