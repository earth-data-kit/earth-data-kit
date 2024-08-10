Using a grid file
-----------------

Grid file is a kml or shapefile which contains a mapping of data provider's grid to world coordinates. 

It contains a ``Name`` column and a ``geometry`` which contains a mapping between ``x`` and ``y`` of satellite grid to bounding boxes in world coordinates. This file can be used to create a set of patterns, 
which then is used to search for scene files. 

.. note::

   This is the advised method as using the grid file we can pinpoint exactly which files to download and stitch together

As an example we will trying to create COGs from modis scene files kept on S3.
We will try to download data for Albania for 1st month of Jan, 2017.

Before moving ahead it's important to go through :ref:`Defining source`

.. literalinclude:: using_a_grid_file.py
    :language: python