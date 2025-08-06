import earth_data_kit as edk
import country_bboxes
import geopandas as gpd
import pandas as pd
import datetime
import os

def get_aoi():
    return country_bboxes.country_bounding_boxes['AL'][1]

def get_timebounds():
    return (datetime.datetime(2023, 12, 28), datetime.datetime(2023, 12, 31))

def get_sentinel_2():
    # Example: How to download data from S3 using the Sentinel-2 L1C public bucket
    source = (
        "s3://sentinel-s2-l1c/tiles/{utm_code}/{lat_band}/{square}/%Y/%-m/%-d/*/*.jp2"
    )
    # File should be downloaded https://sentinels.copernicus.eu/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml and kept inside DATA_DIR
    grid_fp = "/app/data/sentinel-2.kml"
    
    # Read the KML file containing MODIS grid information
    gdf = gpd.read_file(grid_fp)
    # Prepare grid dataframe with columns as present in source variables - utm_code, lat_band, square
    gdf["utm_code"] = gdf["Name"].str[:2]
    gdf["lat_band"] = gdf["Name"].str[2]
    gdf["square"] = gdf["Name"].str[3:]
    
    bbox = get_aoi()
    date_range = get_timebounds()

    # Creating a dataset
    ds = edk.stitching.Dataset("sentinel-2-l1c", source, "s3", True)

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox, gdf)

    # Getting files by scanning S3
    ds.discover(band_locator="filename")

    # Find bands available in the dataset
    print (ds.get_bands())

    # Mosaic the bands
    ds.mosaic(bands=["B04", "B03", "B02"], sync=False, overwrite=True, resolution=(0.0006, -0.0006), dtype="uint16", crs="EPSG:4326")

    # Save the dataset so we don't have to run the above steps again
    ds.save()

    # Returning as dataarray
    return ds.to_dataarray()

def get_era5():
    # Example: How to download data from Earth Engine using the ERA5 Daily aggregates

    # ERA5 Daily aggregates

    # Creating a dataset
    ds = edk.stitching.Dataset("era5-daily", "ECMWF/ERA5_LAND/DAILY_AGGR", "earth_engine", True)

    date_range = get_timebounds()
    bbox = get_aoi()

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial extent
    ds.set_spacebounds(bbox)

    ds.discover()

    print (ds.get_bands())

    # Keep CRS and resolution same as the dataset as it's already nice and clean
    ds.mosaic(bands=["temperature_2m"], sync=False, overwrite=True)

    ds.save()

    return ds.to_dataarray()


if __name__ == "__main__":
    S2_da = get_sentinel_2()

    ERA5_da = get_era5()

    print (S2_da)
    print (ERA5_da)