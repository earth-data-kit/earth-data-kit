import earth_data_kit.stitching.engines.commons as commons
from osgeo import gdal
import earth_data_kit.utilities.geo as geo
import logging

logger = logging.getLogger(__name__)

class Grib2Adapter:
    def __init__(self):
        pass

    def get_metadata(self, gdal_path, band_locator):
        return geo.get_metadata(gdal_path, band_locator)

    def create_tiles(self, scan_df, band_locator):
        metadata = commons.get_tiles_metadata(
            scan_df["gdal_path"].tolist(), band_locator
        )

        print (metadata)