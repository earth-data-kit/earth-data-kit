Stitching
=========

Earth Observation (EO) data is often distributed as many small scene files. These files may vary in their projections, resolutions, and naming conventions depending on the data provider.

For example, Modis (<https://registry.opendata.aws/modis/>) and Sentinel-2 (<https://registry.opendata.aws/sentinel-2/>) both organize their data differently and use different coordinate systems.

Before analysis or visualization, you typically need to:

- Download multiple scene files.
- Rearrange or select specific bands.
- Harmonize resolutions and projections.

The Stitching module simplifies this workflow by enabling users to mosaic and preprocess EO scene files into a unified dataset. The resulting outputs (VRTs) can then be formatted as Cloud Optimized GeoTIFFs (COGs) or Zarr archives for easy use using GDAL.

Dataset Class
-------------
The main class in this module is **Dataset**, which manages the entire stitching process. Below is an example of its usage:

.. code-block:: python

    import earth_data_kit as edk
    ds = edk.stitching.Dataset(id, source, engine)

Parameters:

* ``id``: A string used to create a temporary directory for catalog and intermediate vrts.
* ``source``: A string specifying the data source; its format depends on the chosen engine. (See :ref:`Defining source` for more details.)
* ``engine``: A string that identifies the processing engine. Currently supported value: **s3**, **earth_engine**.

Additional Documentation
------------------------
.. toctree::
    stitching/defining-source
    stitching/using-a-grid-file
    stitching/without-a-grid-file