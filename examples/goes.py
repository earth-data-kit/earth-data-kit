
"""
Example: Accessing and Combining GOES-19 NetCDF Files from AWS S3

This script demonstrates how you can use Earth Data Kit (EDK) to discover and combine
GOES-19 NetCDF files stored in AWS S3. The example covers dataset discovery and combination
for a specified time range. Note this only works on Linux systems
"""

import datetime
import earth_data_kit as edk

def goes19_example():
    """
    Example function to access and combine GOES-19 NetCDF files from AWS S3 using EDK.
    """
    # Define the S3 source template for GOES-19 ABI L2 Cloud Top Height Full Disk data
    source = "s3://noaa-goes19/ABI-L2-ACHTF/%Y/%j/*/*.nc"
    engine = "s3"
    format = "netcdf"

    # Set the time range for data discovery (June 1, 2025 to July 7, 2025)
    timebounds = (datetime.datetime(2025, 6, 1), datetime.datetime(2025, 6, 7))

    # Optionally, define spatial bounds as a bounding box (minx, miny, maxx, maxy)
    # For this example, we use the full disk (no spatial subsetting)
    spatial_bounds = (-180, -90, 180, 90)

    # Create the EDK Dataset object
    ds_name = "goes19"
    ds = edk.stitching.Dataset(
        name=ds_name,
        source=source,
        engine=engine,
        format=format
    )

    # Set the temporal bounds for the dataset
    ds.set_timebounds(timebounds[0], timebounds[1])

    # Optionally, set spatial bounds if you want to subset the data
    ds.set_spacebounds(spatial_bounds)

    # Discover available scenes/files within the specified time range
    ds.discover()

    print("GOES19 bands:", ds.get_bands())
    
    # Example: Mosaic and export the data as a Cloud-Optimized GeoTIFF (COG)
    # You can adjust bands, CRS, and resolution as needed
    # Uncomment the following lines to perform mosaic and export
    ds.mosaic(
        bands=["TEMP"],  # Specify the band(s) to mosaic, e.g., "CMI" for Cloud and Moisture Imagery
        sync=False,
        overwrite=True,
        crs="EPSG:3857",  # Output projection
        resolution=(2000, -2000)  # Output resolution in meters (example: 2km)
    )

    # After discovering, you can get the xarray.DataArray directly:
    da = ds.to_dataarray()
    print(da)

    # Export the mosaicked dataset as a Cloud-Optimized GeoTIFF (COG)
    # Specify the output directory or file path as needed
    # Uncomment the following lines to export
    # output_path = "/app/data/examples/goes19"
    # ds.export(output_path)
    # print(f"GOES19 dataset exported to {output_path}")

if __name__ == '__main__':
    goes19_example()