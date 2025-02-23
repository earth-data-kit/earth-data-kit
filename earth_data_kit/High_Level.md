# Earth Data Kit

* Modules
  * edk.stitching
    * Functions
      - edk.stitching.Dataset()
      - ds.set_timebounds()
      - ds.set_spacebounds()
      - ds.discover()
        * This fills ds.bands with band configuration of the dataset
      - ds.to_vrts()
        * This creates timestamp wise vrts
      - ds.to_cogs()
        * This converts those vrts to COGs
  * edk.xarray
    * Functions
      - edk.xarray.open_dataset()
  * edk.geopandas
    * Functions
      - edk.geopandas.read_file()

* Functions
  * edk.init_cluster()
* Classes
  * edk.stitching.Dataset

# Earth Data Cluster
* Dockerised set of containers to run jobs sent by earth_data_kit.
  1. Capable of splitting and computing parallely
  2. Runs on ray

## Roadmap
* edk.stitching
  * Input Engines supported
    1. S3 - *Implemented*
    2. Earth Engine - *Implemented*
    3. Arbritrary files kept in a folder - *Planned*
    4. Planetary Compute *Planned*
    5. ESGF - https://esgf.llnl.gov/ - *Planned*
  * Output formats
    1. VRTs
    2. COGs
  * Supported Options
    * Resolution
    * Projection
    * No data values
* xarray backend - https://docs.xarray.dev/en/stable/internals/how-to-add-new-backend.html#rst-backend-entrypoint
* xarray-ray integration - https://docs.xarray.dev/en/stable/internals/chunked-arrays.html#internals-chunkedarrays
* xarray-ramba - https://github.com/Python-for-HPC/ramba
