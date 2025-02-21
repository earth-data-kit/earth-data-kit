Google Earth Engine
===================

This example demonstrates how to use the Earth Data Kit to stitch together a Earth Engine collection.


.. code-block:: python

    import earth_data_kit as edk

    # Initialize the Dataset using the Earth Engine.
    dataset_id = "example_earth_engine_dataset"
    # Replace with your actual Earth Engine collection ID (e.g., LANDSAT/LC08/C01/T1_SR).
    source = "LANDSAT/LC08/C01/T1_SR"
    engine = "earth_engine"

    ds = edk.stitching.Dataset(dataset_id, source, engine)

    # Set the time bounds for the dataset.
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime(2020, 12, 31)
    ds.set_timebounds(start_date, end_date)

    # Define the spatial bounding box (min_lon, min_lat, max_lon, max_lat)
    bbox = (-122.75, 36.8, -121.75, 37.8)

    # For Earth Engine collections, grid files are not needed.
    # Passing None for both grid file and extraction function.
    ds.set_spacebounds(bbox, None, None)

    # Discover the available images within the specified temporal and spatial bounds.
    ds.discover()

    # Optionally, configure GDAL options (e.g., setting the target spatial reference).
    ds.set_gdal_options([
        "-t_srs EPSG:3857",
    ])

    # Define the ordered list of band descriptions you wish to stitch together.
    bands = ["B4", "B3", "B2"]  # Typically representing Red, Green, and Blue bands.

    # Create the stitched VRTs from the provided Earth Engine data.
    ds.to_vrts(bands)