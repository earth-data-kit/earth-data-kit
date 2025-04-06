import xarray as xr
import logging
import pandas as pd
from osgeo import gdal
import earth_data_kit as edk
import folium
import matplotlib as mpl
import numpy as np
import earth_data_kit.stitching.decorators as decorators
logger = logging.getLogger(__name__)


def create_cmap(base_color):
    cmap = mpl.colors.LinearSegmentedColormap.from_list(
        "custom_red", ["white", base_color]
    )

    def colormap(x):
        return cmap(x, 1)

    return colormap


def get_raster_bounds(fp):
    _df = pd.read_json(fp)
    df = pd.DataFrame(_df["EDKDataset"]["VRTDatasets"])

    # Getting first vrt to figure out raster bounds
    vrt_fp = df.loc[0, "source"]
    gdal_ds = gdal.Open(vrt_fp)
    gt = gdal_ds.GetGeoTransform()
    width = gdal_ds.RasterXSize
    height = gdal_ds.RasterYSize

    # Get the coordinates of the corners
    ulx = gt[0]
    uly = gt[3]
    lrx = gt[0] + width * gt[1] + height * gt[2]
    lry = gt[3] + width * gt[4] + height * gt[5]
    xmin, ymax, xmax, ymin = ulx, uly, lrx, lry

    # TODO: Make the fetching of CRS dynamic instead of hardcoding it to 3857
    lon_min, lat_min, lon_max, lat_max = edk.utilities.transform.transform_bbox(
        xmin, ymin, xmax, ymax, 3857, 4326
    )

    return lon_min, lat_min, lon_max, lat_max


def scale_to_255(arr):
    np.nanmax(arr)
    np.nanmin(arr)

    # Scale values between 0-255
    # First handle NaN values and identify min/max
    arr_no_nan = arr[~np.isnan(arr)]
    arr_min = np.min(arr_no_nan)
    arr_max = np.max(arr_no_nan)

    # Create a copy to avoid modifying the original array
    arr_scaled = arr.copy()

    # Scale the non-NaN values between 0-255
    if arr_max > arr_min:  # Avoid division by zero if all values are the same
        arr_scaled[~np.isnan(arr)] = (
            (arr_no_nan - arr_min) / (arr_max - arr_min)
        ) * 255

    # TODO: We need a way to handle the NaN values, are not able to convert to unit8
    # # Convert to uint8 for proper image representation
    # arr_scaled = arr_scaled.astype(np.uint8)

    return arr_scaled


def plot_xarray_da(da):
    lon_min, lat_min, lon_max, lat_max = get_raster_bounds(da.attrs["source"])

    m = folium.Map(
        location=[(lat_max + lat_min) / 2, (lon_max + lon_min) / 2], zoom_start=9
    )

    arr = da.values
    band = folium.raster_layers.ImageOverlay(
        image=scale_to_255(arr).T,
        bounds=[[lat_min, lon_min], [lat_max, lon_max]],
        interactive=True,
        colormap=create_cmap("red"),
    )

    band.add_to(m)
    folium.LayerControl().add_to(m)
    return m


@xr.register_dataarray_accessor("edk")
class EDKAccessor:
    def __init__(self, xarray_obj, *args, **kwargs):
        self._da = xarray_obj

    @decorators.log_time
    @decorators.log_init
    def plot(self, time, band, *args, **kwargs):
        _da = self._da.sel(time=time, band=band)
        return plot_xarray_da(_da)
