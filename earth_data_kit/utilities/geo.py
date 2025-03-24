from osgeo import gdal
import logging
from earth_data_kit.stitching import decorators
import shapely

logger = logging.getLogger(__name__)


@decorators.log_time
@decorators.log_init
def set_band_descriptions(filepath, bands):
    """
    Set the descriptions for bands in a GDAL raster file.

    Parameters:
        filepath (str): The path to the raster file to be updated.
        bands (list of str): A list of description strings for each raster band.

    Returns:
        None

    Raises:
        Exception: If the file cannot be opened or updated.
    """
    ds = gdal.Open(filepath, gdal.GA_Update)
    for idx in range(len(bands)):
        rb = ds.GetRasterBand(idx + 1)
        rb.SetDescription(bands[idx])
    del ds

def warp_and_get_extent(df_row):
    ds = gdal.Warp(
        "/vsimem/reprojected.tif", gdal.Open(df_row.gdal_path), dstSRS="EPSG:4326"
    )
    geo_transform = ds.GetGeoTransform()
    x_min = geo_transform[0]
    y_max = geo_transform[3]
    x_max = x_min + geo_transform[1] * ds.RasterXSize
    y_min = y_max + geo_transform[5] * ds.RasterYSize
    polygon = shapely.geometry.box(x_min, y_min, x_max, y_max, ccw=True)
    ds = None
    return polygon