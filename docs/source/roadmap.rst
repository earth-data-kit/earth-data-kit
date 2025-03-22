Roadmap
=======

Below is the tentative roadmap for Earth Data Kit. Our approach is to look closely on how a GIS pipeline is built and fill the critical gaps using existing technologies or building new ones.

.. note::

   We view this roadmap as a collaborative and dynamic blueprint for Earth Data Kit. We hope that community contributions will continuously refine these plans to meet real-world challenges.

1. **Stitching**
    * A module dedicated to collecting and stitching data from varied sources.
    * Prepares data for analysis with functionalities such as clipping, reprojecting, and regridding.
    * Supported Sources:
        - Rasters:
            * S3
            * Google Earth Engine
            * HTTP/HTTP(s) servers (*Planned*)
            * NASA Earth Data (*Planned*)
        - Vectors (*Planned*):
            * OSM
            * Overture Maps
            * Google Buildings
            * Bing Buildings

2. **Visualization**
    - Extend xarray and geopandas to support large scale visualization.

3. **Integration with xarray**
    - Develop a new xarray backend for EDK datasets.
    - Enable lazy evaluation of datasets using Ray.

4. **Integration with geopandas**
    - Introduce lazy evaluation.
    - Support handling of out-of-memory datasets.
    - Leverage Ray for backend processing.

5. **Cloud Support**
    - Run seamlessly on AWS (Lambda, Batch, etc.), GCP, and Azure.
