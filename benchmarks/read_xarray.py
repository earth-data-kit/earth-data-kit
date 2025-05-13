import rioxarray as rio


def read_xarray(vrt_path):
    ds = rio.open_rasterio(vrt_path)
    res = ds.sel(band=1).mean().compute()
    return res
