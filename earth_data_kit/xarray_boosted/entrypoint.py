from xarray.backends import BackendEntrypoint
import os
import glob
import xarray as xr
from osgeo import gdal
import pandas as pd

def open_edk_dataset(filename_or_obj):
    # TODO: Update the path
    pat = f"{filename_or_obj}/pre-processing/????-??-??-??:??:??.vrt"
    vrts = glob.glob(pat)
    print (vrts)

    dses = []
    dates = []
    for fp in vrts:
        dses.append(gdal.Open(fp))
        dates.append(fp.split("/")[-1].split(".")[0])

    das = []
    for ds in dses:
        da = xr.DataArray(ds.ReadAsArray(), dims=("band", "x", "y"), attrs={"spatial_ref": ds.GetProjection()})
        das.append(da)

    res_da = xr.concat(das, dim=pd.DatetimeIndex(dates))
    res_da = res_da.rename(new_name_or_name_dict={"concat_dim": "date"})
    res_ds = res_da.to_dataset(name=filename_or_obj.split("/")[-1])
    return res_ds

class EDKDatasetBackendEntrypoint(BackendEntrypoint):
    def open_dataset(
        self,
        filename_or_obj,
        *,
        drop_variables=None,
        # other backend specific keyword arguments
        # `chunks` and `cache` DO NOT go here, they are handled by xarray
    ):
        return open_edk_dataset(filename_or_obj)

    open_dataset_parameters = ("filename_or_obj", "drop_variables")

    def guess_can_open(self, filename_or_obj):
        try:
            _, ext = os.path.splitext(filename_or_obj) # type: ignore
        except TypeError:
            return False
        return ext in {".my_format", ".my_fmt"}

    description = "Use .my_format files in Xarray"

    url = "https://link_to/your_backend/documentation"
