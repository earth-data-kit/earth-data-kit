"""
Example: Combining Sentinel-2 and ERA5 Datasets with earth-data-kit

This script demonstrates how to use earth-data-kit to download, mosaic, and combine two different geospatial datasets:
- Sentinel-2 Level 2A imagery (from AWS S3)
- ERA5 daily temperature aggregates (from Google Earth Engine)

The workflow includes:
1. Defining an area of interest (AOI) and time range.
2. Downloading and mosaicking Sentinel-2 RGB bands as VRTs.
3. Downloading and mosaicking ERA5 daily temperature as VRTs.
4. Combining the two datasets into a single multi-band dataset, spatially and temporally aligned.
5. Exporting the combined dataset as COGs.

This example is intended to show how you can combine two different datasets using edk.
"""

from fiona.drvsupport import supported_drivers

supported_drivers["LIBKML"] = "rw"

import earth_data_kit as edk
import country_bboxes
import geopandas as gpd
import datetime


def get_aoi():
    # Albania bounding box as example
    return country_bboxes.country_bounding_boxes["AL"][1]


def get_timebounds():
    # Example time range
    return (datetime.datetime(2023, 12, 28), datetime.datetime(2023, 12, 31))


def get_sentinel2_dataset():
    # Sentinel-2 L2A S3 bucket template
    source = "s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/{utm_code}/{lat_band}/{square}/%Y/%-m/*_%Y%m%dT*/*.tif"
    # KML grid file for Sentinel-2 tiling
    grid_fp = "https://sentinels.copernicus.eu/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml/ec05e22c-a2bc-4a13-9e84-02d5257b09a8/app/data/sentinel-2.kml"

    # Read Sentinel-2 grid
    gdf = gpd.read_file(grid_fp)
    gdf["utm_code"] = gdf["Name"].str[:2]
    gdf["lat_band"] = gdf["Name"].str[2]
    gdf["square"] = gdf["Name"].str[3:]

    bbox = get_aoi()
    date_range = get_timebounds()

    # Create EDK dataset for Sentinel-2
    s2_ds = edk.stitching.Dataset("sentinel-2-l2a", source, "s3", "geotiff", True)
    s2_ds.set_timebounds(date_range[0], date_range[1])
    s2_ds.set_spacebounds(bbox, gdf)
    s2_ds.discover(band_locator="filename")
    print("Sentinel-2 bands:", s2_ds.get_bands())

    # Mosaic RGB bands and write as COG
    s2_ds.mosaic(
        bands=["B04", "B03", "B02"],
        sync=False,
        overwrite=True,
        resolution=(0.0006, -0.0006),
        dtype="uint16",
        crs="EPSG:4326",
    )
    s2_ds.save()
    return s2_ds


def get_era5_dataset():
    # ERA5 Daily aggregates from Earth Engine
    era5_source = "ECMWF/ERA5_LAND/DAILY_AGGR"
    date_range = get_timebounds()
    bbox = get_aoi()

    era5_ds = edk.stitching.Dataset(
        "era5-daily", era5_source, "earth_engine", "earth_engine", True
    )
    era5_ds.set_timebounds(date_range[0], date_range[1])
    era5_ds.set_spacebounds(bbox)
    era5_ds.discover()
    print("ERA5 bands:", era5_ds.get_bands())

    # Mosaic temperature_2m and write as COG
    era5_ds.mosaic(
        bands=["temperature_2m"],
        sync=False,
        overwrite=True,
    )
    era5_ds.save()
    return era5_ds


if __name__ == "__main__":
    # Download, mosaic, and save Sentinel-2 and ERA5 as COGs
    s2_ds = get_sentinel2_dataset()
    era5_ds = get_era5_dataset()

    # Print info about the datasets
    print("Sentinel-2 DataArray:", s2_ds.to_dataarray())
    print("ERA5 DataArray:", era5_ds.to_dataarray())

    # Example: Combine the two datasets into a single multi-band dataset
    # This will align the datasets spatially and temporally as much as possible
    combined_ds = edk.stitching.Dataset.combine(
        s2_ds.to_dataarray(), [era5_ds.to_dataarray()]
    )
    print("Combined Dataset:", combined_ds)

    # Export the combined dataset as COGs using the .edk.export function
    combined_ds.edk.export("/app/data/examples/sentinel2-era5")
    print("Combined Dataset exported to /app/data/examples/sentinel2-era5")
