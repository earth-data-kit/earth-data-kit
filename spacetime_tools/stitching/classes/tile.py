from osgeo import gdal
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Tile:
    def __init__(self, engine_path, gdal_path, size) -> None:
        self.engine_path = engine_path
        self.gdal_path = gdal_path
        self.size = size

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
        size,
        geo_transform,
        x_min,
        x_max,
        y_min,
        y_max,
        projection,
        local_path,
    ):
        t = Tile(engine_path, gdal_path, size)
        t.set_metadata(
            {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "geo_transform": geo_transform,
                "projection": projection,
            }
        )
        t.set_local_path(local_path)
        return t

    def get_metadata(self):
        # Figure out aws options
        ds = gdal.Open(self.gdal_path)
        geo_transform = ds.GetGeoTransform()
        x_min = geo_transform[0]
        y_max = geo_transform[3]
        x_max = x_min + geo_transform[1] * ds.RasterXSize
        y_min = y_max + geo_transform[5] * ds.RasterYSize
        projection = ds.GetProjection()

        return {
            "geo_transform": geo_transform,
            "x_min": x_min,
            "y_max": y_max,
            "x_max": x_max,
            "y_min": y_min,
            "projection": projection,
        }
        # bands = []
        # band_count = ds.RasterCount
        # for i in range(1, band_count + 1):
        #     band = ds.GetRasterBand(i)
        #     bands.append(
        #         {
        #             "band_idx": i,
        #             "description": band.GetDescription(),
        #             "geo_transform": geo_transform,
        #             "dtype": band.DataType,
        #             "x_size": band.XSize,
        #             "y_size": band.YSize,
        #             # TODO: Round so that random floating point errors don't come
        #             "x_res": geo_transform[1],
        #             "y_res": geo_transform[5],
        #             "x_min": x_min,
        #             "y_min": y_min,
        #             "x_max": x_max,
        #             "y_max": y_max,
        #             "projection": projection,
        #         }
        #     )
        # return bands

    def set_metadata(self, metadata):
        self.geo_transform = metadata["geo_transform"]
        self.x_min = metadata["x_min"]
        self.x_max = metadata["x_max"]
        self.y_min = metadata["y_min"]
        self.y_max = metadata["y_max"]
        self.projection = metadata["projection"]

    def set_local_path(self, local_path):
        self.local_path = local_path
