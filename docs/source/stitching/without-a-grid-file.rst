Without a grid file
-------------------

When grid file is not available system can try to read tile metadata to do spatial filtering. In such cases users can simply supply ``*`` (wildcard) instead of providing ``h`` and ``v`` parts of source string.

To demonstrate we will try to download data the same data, Albania for 1st month of Jan, 2017 without using a grid file

.. warning::
    This method can be time consuming as metadata of all files is fetched first and then filtered based on bounding box

.. literalinclude:: without_a_grid_file.py
    :language: python