Without a grid file
-------------------

When grid file is not available system can try to read tile metadata to do spatial filtering. In such cases users can simply supply ``*`` (wildcard) instead of providing ``h`` and ``v`` parts of source string.

To demonstrate we will try to download data the same data, Albania for 1st week of Jan, 2017 without using a grid file

.. warning::
    This method can be time consuming as metadata of all files is fetched first and then filtered based on bounding box

.. code-block:: python

    import spacetime_tools

    # Derived from path of a single scene file
    source = "s3://modis-pds/MCD43A4.006/*/*/%Y%j/*_B07.TIF"
    ds = spacetime_tools.DataSet("modis-pds", source, "s3")

    # Modis Data is at a daily frequency so we create one COG per day
    destination = "/<local_path>/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"

    # As an example we will use Albania's bounding box and get data for the month of January 2017 from s3://modis-pds/MCD43A4.006/
    bbox = (19.3044861183, 39.624997667, 21.0200403175, 42.6882473822)
    date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 31)) # (Start, End) - End date is inclusive

    # Setting time bounds
    ds.set_timebounds(date_range[0], date_range[1])

    # Setting spatial bounds. We pass the bbox we are interested in
    ds.set_spacebounds(bbox)

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