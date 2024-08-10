Stitching
=========


Earth Observation data is usually distributed as small scene files, with different projections, resolutions 
across data providers. 

For example Modis data <https://registry.opendata.aws/modis/> and Sentinel-2 data <https://registry.opendata.aws/sentinel-2/> both have different file path layout and are in different projections.

Before using this data it's important to combine different scene files together, re-arrange bands and bring the datasets to consistent resolution and projection.

This module aims to make it easy for anyone to download and combine EO scene files into either COGs or Zarr 
so that they can be visualized or analyzed easily.

The main class implemented is **Dataset**.

.. code-block:: python

    ds = spacetime_tools.DataSet(id, source, engine)

* ``id`` - string, used to create temporary directory for raw scene files and other intermediary files
* ``source`` - string, engine dependent. Read more at - :ref:`Defining source`
* ``engine`` - string, engine identifier, one of possible values. **s3**

.. toctree::
    stitching/defining-source
    stitching/using-a-grid-file
    stitching/without-a-grid-file