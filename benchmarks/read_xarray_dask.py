import xarray as xr
from earth_data_kit.stitching.decorators import log_time, log_init


@log_init
@log_time
def read_xarray_dask(vrt_path, block_multiplier):
    block_size = 128
    # Was not working when tried with chunks = {0: 2, 1: 512, 2: 512}
    ds = xr.open_dataset(
        vrt_path,
        chunks={
            "band": "auto",
            "x": block_size * block_multiplier,
            "y": block_size * block_multiplier,
        },
        lock=False,
    )
    res = ds.mean().compute()
    print(res)
