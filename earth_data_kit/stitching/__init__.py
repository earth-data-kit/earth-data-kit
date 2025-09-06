from earth_data_kit.stitching.classes.dataset import Dataset

import xarray as xr
import numpy as np

def combine(ref_da, das, method=None):
    """
    Combine a list of DataArrays by interpolating each to the grid of the reference DataArray,
    using the specified interpolation methods for each DataArray.

    Parameters
    ----------
    ref_da : xarray.DataArray
        The reference DataArray whose grid will be used for interpolation.
    das : list of xarray.DataArray
        List of DataArrays to combine (excluding the reference DataArray).
    method : str or list of str, optional
        Interpolation method(s) to use for each DataArray in das. If a single string is provided,
        it is used for all DataArrays. If a list is provided, it must be the same length as das.
        Default is "linear" for all.

    Returns
    -------
    xarray.DataArray
        Concatenated DataArray with a new 'band' dimension, with the reference DataArray as the first band.
    """
    if method is None:
        method = ["linear"] * len(das)
    elif isinstance(method, str):
        method = [method] * len(das)
    elif isinstance(method, (list, tuple)):
        if len(method) != len(das):
            raise ValueError("Length of method list must match number of DataArrays in das.")
    else:
        raise TypeError("method must be a string or a list/tuple of strings.")

    interped = [
        da.interp(x=ref_da.x, y=ref_da.y, method=m)
        for da, m in zip(das, method)
    ]
    all_das = [ref_da] + interped
    out = xr.concat(all_das, dim="band", join="outer")
    out = out.assign_coords(band=("band", np.arange(1, out.sizes["band"] + 1)))
    return out