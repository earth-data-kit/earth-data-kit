import rioxarray as rio
from earth_data_kit.stitching.decorators import log_time, log_init


@log_init
@log_time
def read_xarray(vrt_path):
    ds = rio.open_rasterio(vrt_path)
    res = ds.mean().compute()
    print(res)
