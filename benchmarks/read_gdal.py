from osgeo import gdal
from earth_data_kit.stitching.decorators import log_time, log_init


@log_init
@log_time
def read_gdal(vrt_path):
    ds = gdal.Open(vrt_path)
    res = ds.ReadAsArray().mean()
    print(res)
