from osgeo import gdal
import logging
import pandas as pd
import earth_data_kit.stitching.decorators as decorators
import json
import uuid
from earth_data_kit.stitching.classes.band import Band

gdal.UseExceptions()

logger = logging.getLogger(__name__)


class Tile:
    def __init__(self, engine_path, gdal_path, date, tile_name) -> None:
        self.engine_path = engine_path
        self.gdal_path = gdal_path
        self.date = date
        self.tile_name = f"{tile_name}-{uuid.uuid4()}"

        self.set_metadata(self.fetch_remote_metadata())

    @staticmethod
    def to_df(tiles):
        df = pd.DataFrame([t.__dict__ for t in tiles])
        return df

    @decorators.log_time
    @decorators.log_init
    def fetch_remote_metadata(self):
        # Figure out aws options
        ds = gdal.Open(self.gdal_path)
        geo_transform = ds.GetGeoTransform()
        x_min = geo_transform[0]
        y_max = geo_transform[3]
        x_max = x_min + geo_transform[1] * ds.RasterXSize
        y_min = y_max + geo_transform[5] * ds.RasterYSize
        projection = ds.GetProjection()
        length_unit = ds.GetSpatialRef().GetAttrValue("UNIT")

        bands = json.dumps(self.get_bands(ds))

        # Getting reprojected raster's extent. This is done so that we can filter later on
        warped_ds = gdal.Warp(f"/vsimem/{uuid.uuid4()}.tif", ds, dstSRS="EPSG:4326")

        wgs_geo_transform = warped_ds.GetGeoTransform()
        wgs_x_min = wgs_geo_transform[0]
        wgs_y_max = wgs_geo_transform[3]
        wgs_x_max = wgs_x_min + wgs_geo_transform[1] * warped_ds.RasterXSize
        wgs_y_min = wgs_y_max + wgs_geo_transform[5] * warped_ds.RasterYSize
        warped_ds = None
        o = {
            "geo_transform": geo_transform,
            "x_min": x_min,
            "y_max": y_max,
            "x_max": x_max,
            "y_min": y_min,
            "x_res": geo_transform[1],
            "y_res": geo_transform[5],
            "projection": projection,
            "wgs_x_min": wgs_x_min,
            "wgs_y_min": wgs_y_min,
            "wgs_x_max": wgs_x_max,
            "wgs_y_max": wgs_y_max,
            "bands": bands,
            "length_unit": length_unit,
        }
        return o

    def set_metadata(self, metadata):
        self.geo_transform = metadata["geo_transform"]
        self.x_min = metadata["x_min"]
        self.x_max = metadata["x_max"]
        self.y_min = metadata["y_min"]
        self.y_max = metadata["y_max"]
        self.x_res = metadata["x_res"]
        self.y_res = metadata["y_res"]
        self.projection = metadata["projection"]
        self.bands = metadata["bands"]

        self.wgs_x_min = metadata["wgs_x_min"]
        self.wgs_x_max = metadata["wgs_x_max"]
        self.wgs_y_min = metadata["wgs_y_min"]
        self.wgs_y_max = metadata["wgs_y_max"]

        self.length_unit = metadata["length_unit"]

    def get_local_path(self):
        return self.local_path

    def set_local_path(self, local_path):
        self.local_path = local_path

    def get_bands(self, ds):
        bands = []
        band_count = ds.RasterCount
        for i in range(1, band_count + 1):
            band = ds.GetRasterBand(i)
            b = Band(
                i,
                (
                    band.GetDescription()
                    if band.GetDescription() != ""
                    else "NoDescription"
                ),
                gdal.GetDataTypeName(band.DataType),
                band.XSize,
                band.YSize,
            )
            bands.append(b)
        return bands
