"""
Example: Download and mosaic Sentinel-2 imagery from AWS S3 using Earth Data Kit (EDK)

This script shows how you can:
- Define an area of interest (AOI) and time range
- Use a Sentinel-2 grid KML to select relevant tiles
- Discover and mosaic Sentinel-2 L2A bands directly from AWS S3 via EDK
- Export the resulting mosaic

Requirements:
- The file 'sentinel-2.kml' must be present in your data directory (see grid_fp below).
- Update paths as needed for your environment.
"""

from fiona.drvsupport import supported_drivers
supported_drivers['LIBKML'] = 'rw'

import earth_data_kit as edk
import country_bboxes
import geopandas as gpd
import datetime
import os

def get_aoi():
    """
    Returns the bounding box for Albania as (minx, miny, maxx, maxy).
    You can change the country code to select a different AOI.
    """
    return country_bboxes.country_bounding_boxes["AL"][1]

def get_timebounds():
    """
    Returns the time range for the data query.
    """
    return (datetime.datetime(2023, 12, 28), datetime.datetime(2023, 12, 31))

def get_sentinel_2():
    """
    Downloads, discovers, and mosaics Sentinel-2 L2A imagery for the AOI and time range.
    Returns the result as an xarray.DataArray.
    """
    # Sentinel-2 L2A AWS Open Data S3 path template
    source = (
        "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/"
        "{utm_code}/{lat_band}/{square}/%Y/%-m/*_%Y%m%dT*/*.tif"
    )

    # Path to the Sentinel-2 grid KML file
    grid_fp = "https://sentinels.copernicus.eu/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml/ec05e22c-a2bc-4a13-9e84-02d5257b09a8"  # Make sure this file exists

    # Read the Sentinel-2 grid KML file
    gdf = gpd.read_file(grid_fp)
    # Extract UTM grid codes from the 'Name' field
    gdf["utm_code"] = gdf["Name"].str[:2]
    gdf["lat_band"] = gdf["Name"].str[2]
    gdf["square"] = gdf["Name"].str[3:]

    bbox = get_aoi()
    date_range = get_timebounds()

    # Create a Dataset object for Sentinel-2
    ds = edk.stitching.Dataset(
        "sentinel-2-l2a", source, "s3", "geotiff", True
    )

    # Set the time and spatial bounds
    ds.set_timebounds(date_range[0], date_range[1])
    ds.set_spacebounds(bbox, gdf)

    # Discover available files (scans S3 for matching tiles)
    ds.discover(band_locator="filename")

    # Print available bands
    print("Available bands:", ds.get_bands())

    # Mosaic the selected bands (RGB: B04, B03, B02)
    ds.mosaic(
        bands=["B04", "B03", "B02"],
        sync=False,
        overwrite=True,
        resolution=(0.0006, -0.0006),
        dtype="uint16",
        crs="EPSG:4326",
    )

    # Save the dataset metadata for future use
    ds.save()

    # Return the mosaic as an xarray.DataArray
    return ds.to_dataarray()

if __name__ == "__main__":
    # Run the example workflow
    print("Starting Sentinel-2 download and mosaic example...")

    S2_da = get_sentinel_2()

    print("Mosaic complete. DataArray shape:", S2_da.shape)
    print("Exporting mosaic to ./sentinel2 ...")

    # Export the mosaic to the specified directory
    S2_da.edk.export("/app/data/examples/sentinel2")

    print("Export complete. Files saved in ./sentinel2")
