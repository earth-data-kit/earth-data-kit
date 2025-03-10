import read_xarray
import read_gdal
import read_xarray_dask
import read_gdal_parallel
import read_edk

if __name__ == "__main__":
    base_path = "/Users/siddhantgupta3/Desktop/Work/project-planet-bench/repos/private/earth-data-kit/tmp/tmp/modis-pds-india"
    vrt_path = f"{base_path}/pre-processing/2017-01-01-00:00:00.vrt"

    edk_fp = f"{base_path}/modis-pds-india.xml"
    # read_xarray.read_xarray(vrt_path) # 209s
    # read_gdal.read_gdal(vrt_path) # 200s
    # read_xarray_dask.read_xarray_dask(vrt_path, block_multiplier=8)  # 222.111
    # read_gdal_parallel.read_gdal_parallel(vrt_path, block_multiplier=8, max_workers=10) # block_size = 128, block_multiplier = 8, 10 threads: 60s
    # read_gdal_parallel.read_gdal_parallel(vrt_path, block_multiplier=4) # block_size = 128, block_multiplier = 4, 10 threads: 72s
    # read_gdal_parallel.read_gdal_parallel(vrt_path, block_multiplier=4, max_workers=16) # block_size = 128, block_multiplier = 4, 16 threads: 72s
    read_edk.read_edk(edk_fp, block_multiplier=8)
