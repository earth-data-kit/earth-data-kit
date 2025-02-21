from osgeo import gdal
import logging
from earth_data_kit.stitching import decorators

logger = logging.getLogger(__name__)


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
