.. Earth Data Kit documentation master file, created by
   sphinx-quickstart on Fri Aug  9 16:16:39 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Earth Data Kit
=============================

Tools to make Geospatial Analysis easy and cheap

.. warning::

   This project is under active development.

Modules
-------

* **Stitching** - Can be used to combine multiple scene files, also called tiles, from a variety of remote data sources, eg: S3, Earth Engine, HTTP/HTTPs servers, etc.

**Input sources**

1. S3 - *Implemented*
2. Google Earth Engine - *Planned*
3. ESGF <https://esgf.llnl.gov/> - *Planned*

**Output formats**

1. COGs (Cloud Optimized Geotiff) - *Implemented*
2. Zarr - *Planned*

Contents
--------
.. toctree::

   getting-started
   stitching