Using a Grid File
-----------------

A grid file is typically a KML or Shapefile that maps a data provider's grid system to global coordinates. 

Such a file usually includes a "Name" column to identify each grid cell and a "geometry" column that defines the corresponding bounding box in world coordinates.

.. note::
   By using a grid file, you can generate targeted search patterns that precisely identify the scene files you need. This is the recommended approach because it allows you to download and stitch together only the relevant files.

When using a grid file, you need to create a grid dataframe that contains:

1. Columns that match the placeholders in your source pattern (e.g., ``{h}``, ``{v}``, ``{p}``, ``{r}``)
2. A ``geometry`` column with the spatial extent of each grid cell

This grid dataframe can then be passed to the ``ds.set_spacebounds()`` method along with your area of interest to automatically determine which grid cells to include in your search.

The example below demonstrates how to create Cloud Optimized GeoTIFFs (COGs) from MODIS scene files stored on S3 by selecting data for Albania during January 2017.

Before proceeding, please review the :ref:`Defining source` section for details on how to define your source pattern.

.. literalinclude:: using_a_grid_file.py
    :language: python