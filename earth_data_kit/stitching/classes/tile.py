from osgeo import gdal
import logging
import pandas as pd
import earth_data_kit.stitching.decorators as decorators
import json

gdal.UseExceptions()

logger = logging.getLogger(__name__)


class Tile:
    def __init__(self, engine_path, gdal_path) -> None:
        self.engine_path = engine_path
        self.gdal_path = gdal_path

    @staticmethod
    def to_df(tiles):
        df = pd.DataFrame([t.__dict__ for t in tiles])
        return df

    @staticmethod
    def as_tiles(df):
        tiles = [Tile.from_dict(**kwargs) for kwargs in df.to_dict(orient="records")]
        return tiles

    @staticmethod
    def from_dict(
        engine_path,
        gdal_path,
        geo_transform,
        x_min,
        x_max,
        y_min,
        y_max,
        x_res,
        y_res,
        projection,
        local_path,
        bands,
    ):
        t = Tile(engine_path, gdal_path)
        t.set_metadata(
            {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "x_res": x_res,
                "y_res": y_res,
                "geo_transform": geo_transform,
                "projection": projection,
                "bands": bands,
            }
        )
        t.set_local_path(local_path)
        return t

    @decorators.log_time
    @decorators.log_init
    def get_metadata(self):
        # Figure out aws options
        ds = gdal.Open(self.gdal_path)
        geo_transform = ds.GetGeoTransform()
        x_min = geo_transform[0]
        y_max = geo_transform[3]
        x_max = x_min + geo_transform[1] * ds.RasterXSize
        y_min = y_max + geo_transform[5] * ds.RasterYSize
        projection = ds.GetProjection()

        bands = json.dumps(self.get_bands(ds))
        o = {
            "geo_transform": geo_transform,
            "x_min": x_min,
            "y_max": y_max,
            "x_max": x_max,
            "y_min": y_min,
            "x_res": geo_transform[1],
            "y_res": geo_transform[5],
            "projection": projection,
            "bands": bands,
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

    def get_local_path(self):
        return self.local_path

    def set_local_path(self, local_path):
        self.local_path = local_path

    def get_bands(self, ds):
        bands = []
        band_count = ds.RasterCount
        for i in range(1, band_count + 1):
            band = ds.GetRasterBand(i)
            bands.append(
                {
                    "band_idx": i,
                    "description": band.GetDescription(),
                    "dtype": band.DataType,
                    "x_size": band.XSize,
                    "y_size": band.YSize,
                }
            )
        return bands
