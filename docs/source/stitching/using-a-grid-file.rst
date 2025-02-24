Using a Grid File
-----------------

A grid file is typically a KML or Shapefile that maps a data provider's grid system to global coordinates. 

Such a file usually includes a “Name” column to identify each grid cell and a “geometry” column that defines the corresponding bounding box in world coordinates.

.. note::
   By using a grid file, you can generate targeted search patterns that precisely identify the scene files you need. This is the recommended approach because it allows you to download and stitch together only the relevant files.

The example below demonstrates how to create Cloud Optimized GeoTIFFs (COGs) from MODIS scene files stored on S3 by selecting data for Albania during January 2017.

Before proceeding, please review the :ref:`Defining source` section for details on how to define your source pattern.

.. literalinclude:: using_a_grid_file.py
    :language: python