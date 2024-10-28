# Earth Data Kit

* Modules
  * edk.stitching
    * Functions
      - edk.DataSet()
      - ds.set_timebounds()
      - ds.set_spacebounds()
      - ds.discover()
        * This fills ds.bands with band configuration of the dataset
      - ds.to_vrts()
        * This creates timestamp wise vrts
  * edk.xarray
    * Functions
      - edk.xarray.open_dataset()
  * edk.geopandas
    * Functions
      - edk.geopandas.read_file()

* Functions
  * edk.init_cluster()
* Classes
  * edk.DataSet

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
    4. ESGF - https://esgf.llnl.gov/ - *Planned*
  * Output formats
    1. VRTs
