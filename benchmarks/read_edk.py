import xarray as xr
from earth_data_kit.stitching.decorators import log_time, log_init


@log_init
@log_time
def read_edk(vrt_path, block_multiplier):
    block_size = 128
    ds = xr.open_dataset(
        vrt_path,
        engine="edk_dataset",
        chunks={
            "time": 1,
            "band": "auto",
            "x": block_size * block_multiplier,
            "y": block_size * block_multiplier,
        },
    )

    res = ds.sel(time="2017-01-01").mean().compute()
    print(res)
