from osgeo import gdal, osr
import logging
import pandas as pd
import earth_data_kit.stitching.decorators as decorators
import json
import uuid
from earth_data_kit.stitching.classes.band import Band

gdal.UseExceptions()

logger = logging.getLogger(__name__)


class Tile:
    def __init__(self, engine_path, gdal_path, date, tile_name, metadata=None) -> None:
        self.engine_path = engine_path
        self.gdal_path = gdal_path
        self.date = date
        self.tile_name = f"{tile_name}-{uuid.uuid4()}"

        if not metadata:
            metadata = self.fetch_metadata()

        self.geo_transform = metadata["geo_transform"]
        self.projection = metadata["projection"]
        self.bands = metadata["bands"]
        self.wgs_geo_transform = metadata["wgs_geo_transform"]
        self.length_unit = metadata["length_unit"]
        self.x_size = metadata["x_size"]
        self.y_size = metadata["y_size"]
        self.wgs_x_size = metadata["wgs_x_size"]
        self.wgs_y_size = metadata["wgs_y_size"]
        self.crs = metadata["crs"]

    @staticmethod
    def to_df(tiles):
        df = pd.DataFrame([t.__dict__ for t in tiles])
        return df

    @staticmethod
    def from_df(df):
        tiles = []
        for row in df.itertuples():
            tile = Tile(
                row.engine_path,
                row.gdal_path,
                row.date,
                row.tile_name,
                {
                    "geo_transform": row.geo_transform,
                    "projection": row.projection,
                    "wgs_geo_transform": row.wgs_geo_transform,
                    "length_unit": row.length_unit,
                    "x_size": row.x_size,
                    "y_size": row.y_size,
                    "wgs_x_size": row.wgs_x_size,
                    "wgs_y_size": row.wgs_y_size,
                    "crs": row.crs,
                    "bands": row.bands,
                },
            )
            tile.tile_name = row.tile_name
            tiles.append(tile)

        return tiles

    @decorators.log_time
    @decorators.log_init
    def fetch_metadata(self):
        # Figure out aws options
        ds = gdal.Open(self.gdal_path)

        # Getting reprojected raster's extent. This is done so that we can filter later on
        # TODO: Optimize this, we actually don't need to reproject just to get the raster extent.
        warped_ds = gdal.Warp(f"/vsimem/{uuid.uuid4()}.vrt", ds, dstSRS="EPSG:4326", format="VRT")

        o = {
            "geo_transform": ds.GetGeoTransform(),
            "x_size": ds.RasterXSize,
            "y_size": ds.RasterXSize,
            "wgs_x_size": warped_ds.RasterXSize,
            "wgs_y_size": warped_ds.RasterYSize,
            "projection": ds.GetProjection(),
            "crs": "EPSG:"
            + osr.SpatialReference(ds.GetProjection()).GetAttrValue("AUTHORITY", 1),
            "wgs_geo_transform": warped_ds.GetGeoTransform(),
            "bands": json.dumps(self.get_bands(ds)),
            "length_unit": ds.GetSpatialRef().GetAttrValue("UNIT"),
        }

        warped_ds = None
        ds = None
        return o

    def get_extent(self):
        x_min = self.geo_transform[0]
        y_max = self.geo_transform[3]
        x_max = x_min + self.geo_transform[1] * self.x_size
        y_min = y_max + self.geo_transform[5] * self.y_size

        return (x_min, y_min, x_max, y_max)

    def get_wgs_extent(self):
        wgs_x_min = self.wgs_geo_transform[0]
        wgs_y_max = self.wgs_geo_transform[3]
        wgs_x_max = wgs_x_min + self.wgs_geo_transform[1] * self.wgs_x_size
        wgs_y_min = wgs_y_max + self.wgs_geo_transform[5] * self.wgs_y_size

        return (wgs_x_min, wgs_y_max, wgs_x_max, wgs_y_min)

    def get_res(self):
        return (self.geo_transform[1], self.geo_transform[5])

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
                band.GetNoDataValue(),
            )
            bands.append(b)
        return bands
