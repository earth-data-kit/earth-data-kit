.. Earth Data Kit documentation master file, created by
   sphinx-quickstart on Fri Aug  9 16:16:39 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Earth Data Kit
=============================

Today, the GIS world is filled with many great tools and technologies. Each tool is specialized — some offer powerful mapping features, while others excel at data visualization. 

However, when you try to build a complete end-to-end data pipeline, the process becomes challenging, time-consuming and costly. This common struggle inspired us to create Earth Data Kit. 

EDK is designed to bridge these gaps, helping you connect various parts of the GIS process with ease. With Earth Data Kit, data scientists can focus on analyzing data and drawing insights instead of wrestling with complex data processes and engineering challenges.

Check out our :ref:`Roadmap <roadmap>` to see what we're working on and what is planned.

Check out the :ref:`Getting Started` to learn how to install and use Earth Data Kit.

.. warning::

   This project is under active development.

Modules 
-------

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
   roadmap
   developer-docs
   api-reference