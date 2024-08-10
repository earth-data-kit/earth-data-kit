Using a grid file
-----------------

Grid file is a kml or shapefile which contains a mapping of data provider's grid to world coordinates. 

It contains a ``Name`` column and a ``geometry`` which contains a mapping between ``x`` and ``y`` of satellite grid to bounding boxes in world coordinates. This file can be used to create a set of patterns, 
which then is used to search for scene files. 

.. note::

   This is the advised method as using the grid file we can pinpoint exactly which files to download and stitch together

As an example we will trying to create COGs from modis scene files kept on S3.
We will try to download data for Albania for 1st week of Jan, 2017.

Before moving ahead it's important to go through :ref:`Defining source`

.. code-block:: python

    import spacetime_tools
    import re


    # Derived from path of a single scene file
    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF"
    ds = spacetime_tools.DataSet("modis-pds", source, "s3")

    # Modis Data is at a daily frequency so we create one COG per day
    destination = "/<local_path>/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"

    # As an example we will use Albania's bounding box and get data for the month of January 2017 from s3://modis-pds/MCD43A4.006/
    bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 31)) # (Start, End) - End date is inclusive

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # To get the h and v components from file path we need to define a 
    # lambda function which can extract h and v from the grid dataframe as a dictionary
    # Example below
    def fn(df_row):
        match = re.search(r"h:(\d*) v:(\d*)", df_row.Name)
        if match and match.groups():
            vars = match.groups()
            return {
                "h": f"{int(vars[0]):02d}",
                "v": f"{int(vars[1]):02d}",
            }

    # Setting spatial bounds. We pass the bbox we are interested in,
    # grid file path and the lambda function to extract h and v from the grid file
    ds.set_spacebounds(bbox, grid_fp, fn)

    # Getting distinct bands. This will help us decide band arrangement when stitching scenes together
    bands = ds.get_distinct_bands()
    print (bands)

    # Downloading scene files
    ds.sync()

    # Finally stitching them together with the band arrangement as below
    ds.to_cog(
        destination,
        bands=[
            "Nadir_Reflectance_Band7",
        ],
    )