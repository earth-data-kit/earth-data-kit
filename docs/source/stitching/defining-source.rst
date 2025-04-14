Defining Source
===============

The ``source`` parameter in Earth Data Kit can be defined in multiple ways depending on your needs and the engine you're using.

Source Types
------------

Earth Data Kit supports three main ways to define a source:

1. **Single File Path**: A direct path to a single data file::

    source = "s3://my-bucket/path/to/file.tif"

2. **Multiple Files as Array**: A list of specific file paths::

    source = ["s3://my-bucket/file1.tif", "s3://my-bucket/file2.tif"]

3. **Pattern-based Source**: A template with placeholders for matching multiple files::

    source = "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*.TIF"

Note that you cannot mix patterns within an array - if using an array, each element must be a direct file path.

Engine-Specific Source Definitions
----------------------------------

The format of your source definition depends on the engine you're using:

- **Storage-based Engines** (AWS S3, filesystem, http(s), FTP): These engines support pattern-based sources that can include time and spatial components.

- **Earth Engine**: Requires the collection name as defined in Google Earth Engine::

    source = "COPERNICUS/S2_SR_HARMONIZED"  # Example: Sentinel-2 Surface Reflectance Harmonized

Time and Spatial Components in Source Patterns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For storage-based engines, you can include time and spatial components in your source pattern:

- **Time Components**: Use standard Python datetime format codes (e.g., ``%Y`` for 4-digit year, ``%m`` for month) (see https://docs.python.org/3/library/datetime.html#format-codes for reference)::

    source = "s3://my-bucket/data/%Y/%m/%d/*.tif"  # Matches year/month/day folders

- **Spatial Components**: Use placeholders like ``{h}`` and ``{v}`` for horizontal and vertical grid coordinates. Note that you will have to provide the grid file for the remote dataset too::

    source = "s3://landsat-pds/c1/L8/{p}/{r}/%Y/%m/%d/*.TIF"  # {p} for path, {r} for row

These patterns work in conjunction with the ``set_timebounds()`` and ``set_spacebounds()`` methods to filter data based on your specified criteria.

For a detailed example of using pattern-based sources with MODIS data, see :doc:`example-modis-pds`.

.. toctree::
   :maxdepth: 1
   :hidden:

   example-modis-pds
