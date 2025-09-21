"""
Example: Download, Mosaic, and Export GOES-19 NetCDF Data from AWS S3 using Earth Data Kit (EDK)

This example demonstrates how to:
- Define a time range and spatial extent for GOES-19 data
- Discover available NetCDF files on AWS S3 using EDK
- Mosaic the "TEMP" band into a single rasters
- Export the result as a Cloud-Optimized GeoTIFF (COG)

Requirements:
- Linux system (NetCDF S3 access doesn't work on Windows/Mac)
"""

import datetime
import earth_data_kit as edk


def main():
    # Define the S3 path template for GOES-19 NetCDFs
    source = "s3://noaa-goes19/ABI-L2-ACHA2KMF/%Y/%j/%H/*_s%Y%j%H%M*.nc"
    engine = "s3"
    format = "netcdf"

    # Set the time range (UTC) and spatial bounds (lon/lat in EPSG:4326)
    time_start = datetime.datetime(2025, 6, 1)
    time_end = datetime.datetime(2025, 6, 2)
    spatial_bounds = (-170, -81, -50, 81)  # (min_lon, min_lat, max_lon, max_lat)

    # Create the EDK Dataset
    ds = edk.stitching.Dataset("goes19", source, engine, format, clean=True)
    ds.set_timebounds(time_start, time_end)
    ds.set_spacebounds(spatial_bounds)

    # Discover available files/tiles
    ds.discover()

    # Print available bands
    print("Available bands:", ds.get_bands())

    # Mosaic the "TEMP" band to a single raster
    ds.mosaic(
        bands=["TEMP"],
        sync=False,
        overwrite=True,
        crs="EPSG:3857",  # Project to Web Mercator
        resolution=(2000, 2000),  # Output pixel size (meters)
    )
    ds.save()

    # Convert to xarray.DataArray
    da = ds.to_dataarray()

    print(da)

    # Uncomment to export the single timestamp as a Cloud-Optimized GeoTIFF (COG)
    # output_path = "/app/data/examples/goes19/2025-06-01-00:00:00.tif"
    # da.sel(time="2025-06-01T00:00:00").edk.export(output_path)
    # print(f"GOES-19 mosaic exported to {output_path}")


if __name__ == "__main__":
    main()
