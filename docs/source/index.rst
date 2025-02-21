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
----------------

* **Stitching** - Combines multiple scene files (tiles) from a variety of remote data sources such as S3, Google Earth Engine, and HTTP/HTTPS servers.

Input Data Sources
------------------

1. S3 - *Implemented*
2. Google Earth Engine - *Implemented*
3. HTTP/HTTPs Servers - *Planned*

Output Formats
--------------

1. Virtual Raster (VRT) - *Implemented*

Contents
--------
.. toctree::
   :maxdepth: 2

   getting-started
   stitching
   examples
   api-reference