Without a Grid File
-------------------

If a grid file is not available, the system will use tile metadata to perform spatial filtering. 

In this approach, you simply replace the specific grid indices (``h`` and ``v``) in the source string with a wildcard (``*``).

This example demonstrates how to download data for Albania during January 2017 without using a grid file.

.. warning::
   This method can be slower because it first fetches metadata for all available files before applying the bounding box filter.

.. literalinclude:: without_a_grid_file.py
    :language: python