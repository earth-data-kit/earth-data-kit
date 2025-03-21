# Earth Data Kit

EDK is designed to simplify building complete end-to-end data pipeline, helping you connect various parts of the GIS process with ease. With Earth Data Kit, data scientists can focus on analyzing data and drawing insights instead of wrestling with complex data processes and engineering challenges.

## Getting Started

### Prerequisites

Before using Earth Data Kit, ensure that the following are installed:

* Python 3.12 or newer

* GDAL 3.10 or above - [https://gdal.org/en/stable/download.html#binaries](https://gdal.org/en/stable/download.html#binaries)

* s5cmd - [https://github.com/peak/s5cmd](https://github.com/peak/s5cmd)

### Installation

To install Earth Data Kit, follow these steps:

1. Download the latest release from the official GitHub releases page:

    * [https://github.com/earth-data-kit/earth-data-kit/releases](https://github.com/earth-data-kit/earth-data-kit/releases)

2. After downloading the tarball, install the package using pip. For example, within your virtual environment execute:

    * `(.venv) $ wget https://github.com/earth-data-kit/earth-data-kit/releases/download/0.1.1/earth_data_kit-0.1.1.tar.gz`
    * `(.venv) $ pip3 install earth_data_kit-0.1.1.tar.gz`

### Example

```python
import earth_data_kit as edk

# Initialize the Dataset using the S3 engine.
dataset_id = "example_s3_dataset"
# Replace the bucket name and path with your actual S3 location.
source = "s3://your-bucket-name/path/to/data"
engine = "s3"

ds = edk.stitching.Dataset(dataset_id, source, engine)

# Set the temporal bounds for the dataset (e.g., using January 2017 as an example)
start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2017, 1, 31)
ds.set_timebounds(start_date, end_date)

# Define the spatial bounding box (min_lon, min_lat, max_lon, max_lat)
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)
# Specify the grid file that maps the dataset's grid system (e.g., a KML file)
grid_file = "path/to/grid/file.kml"

# Define a function to extract grid components (horizontal 'h' and vertical 'v') from a grid file row.
def extract_grid_components(row):
    import re
    match = re.search(r"h:(\d+)\s+v:(\d+)", row.Name)
    if match:
        return {"h": f"{int(match.group(1)):02d}", "v": f"{int(match.group(2)):02d}"}
    return {}

ds.set_spacebounds(bbox, grid_file, extract_grid_components)

# Discover the available scene files within the defined spatial and temporal bounds.
ds.discover()

# Retrieve and display the list of available bands from the dataset.
available_bands = ds.get_bands()
print("Discovered bands:", available_bands)
# Optionally, configure GDAL options (e.g., setting the target spatial reference).
ds.set_gdal_options([
    "-t_srs EPSG:3857",
])

# Define the ordered list of band descriptions you wish to stitch together.
bands = ["red", "green", "blue"]

# Create the stitched VRTs from the provided S3 data.
ds.to_vrts(bands)
```

## Warning

### This project is under active development

**If you wish to contribute please reach out on <siddhantgupta3@gmail.com>**

Checkout the [docs](https://earth-data-kit.github.io/) for more details.

Checkout the [examples](https://github.com/earth-data-kit/edk-examples).

Checkout the [roadmap](https://earth-data-kit.github.io/roadmap.html).