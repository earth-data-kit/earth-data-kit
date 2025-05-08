import xarray as xr
import earth_data_kit as edk


def read_edk(json_path):
    da = edk.stitching.Dataset.dataarray_from_file(json_path)
    res = da.sel(time="2017-01-01", band=1).mean().compute()
    return res
