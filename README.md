# Earth Data Kit

EDK is designed to simplify building complete end-to-end data pipeline, helping you connect various parts of the GIS process with ease. With Earth Data Kit, data scientists can focus on analyzing data and drawing insights instead of wrestling with complex data processes and engineering challenges.

## Getting Started

### Prerequisites

Before using Earth Data Kit, ensure that the following are installed:

* Python 3.12 or newer

* GDAL 3.8.4 or above - [https://gdal.org/en/stable/download.html#binaries](https://gdal.org/en/stable/download.html#binaries)

* s5cmd (optional) - [https://github.com/peak/s5cmd](https://github.com/peak/s5cmd) - Required if you plan to query data from S3 buckets

### Installation

To install Earth Data Kit, follow these steps:

1. Clone the GitHub repository:

    ```bash
    git clone https://github.com/earth-data-kit/earth-data-kit.git
    cd earth-data-kit
    ```

2. Switch to the master branch:

    ```bash
    git checkout master
    ```

3. Run the installation script:

    ```bash
    bash install.sh
    ```

This will check prerequisites, download the latest tarball, and install Earth Data Kit automatically.

## Example

### Stitching MODIS Dataset from AWS S3

This example demonstrates how to stitch together MODIS satellite imagery from AWS S3 for Albania between January 1-7, 2017. It shows the complete workflow from initializing the dataset to visualizing the results.

```python
import earth_data_kit as edk
import datetime
import os

os.environ["TMP_DIR"] = "./edk-tmp"
os.environ["AWS_REGION"] = "us-west-2"
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
os.environ["EDK_MAX_WORKERS"] = "8"

# Initialize the modis dataset using the S3 engine.
source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF"
dataset_id = "modis-pds"
engine = "s3"

ds = edk.stitching.Dataset(dataset_id, source, engine, clean=True)

# Set the temporal bounds for the dataset (e.g., using January 2017 as an example)
start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2017, 1, 7)
ds.set_timebounds(start_date, end_date)

# Setting spatial bounds for the dataset

# Specify the grid file that maps the dataset's grid system to world coordinates (e.g., a KML/KMZ file)
# kmz is a zipped kml file, so we need to use the vsizip driver to unzip it and curl driver as it's hosted on the web
grid_fp = "/vsizip/vsicurl/https://modis.ornl.gov/files/modis_sin.kmz"
gdf = gpd.read_file(grid_fp)
# Creating grid dataframe with h and v columns
gdf['h'] = gdf['Name'].str.split(' ').str[0].str.split(':').str[1].astype(int).astype(str).str.zfill(2)
gdf['v'] = gdf['Name'].str.split(' ').str[1].str.split(':').str[1].astype(int).astype(str).str.zfill(2)

# Bounding box for Albania, you can change it to any other bbox.
bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)

ds.set_spacebounds(bbox, grid_dataframe=gdf)

# Running discover to get the bands available in the dataset, you can also set gdal options if needed
# ds.set_src_options({"-srcnodata": "32767"})
ds.discover()

# Get the bands discovered in the dataset
ds.get_bands()

# Optionally, configure GDAL options (e.g., setting the target spatial reference).
ds.set_target_options({"-t_srs": "EPSG:3857"})
# Stitches the scene files into VRTs using the defined band arrangement.
ds.mosaic(bands=["Nadir_Reflectance_Band3", "Nadir_Reflectance_Band4"])

# This returns a dataarray with the stitched bands
da = ds.to_dataarray()
```

## Warning

### This project is under active development

**If you wish to contribute please reach out on <siddhantgupta3@gmail.com>**

Checkout the [docs](https://earth-data-kit.github.io/) for more details.

Checkout the [examples](https://github.com/earth-data-kit/edk-examples).

Checkout the [roadmap](https://earth-data-kit.github.io/roadmap.html).