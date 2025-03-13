from osgeo import gdal, osr
import logging
import pandas as pd
import earth_data_kit.stitching.decorators as decorators
import json
import uuid
from earth_data_kit.stitching.classes.band import Band
import numpy as np
import earth_data_kit.utilities as utilities

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
        self.length_unit = metadata["length_unit"]
        self.x_size = metadata["x_size"]
        self.y_size = metadata["y_size"]
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
                    "length_unit": row.length_unit,
                    "x_size": row.x_size,
                    "y_size": row.y_size,
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
        """
        Retrieve metadata for this tile.

        Opens the GDAL dataset using the tile's gdal_path, extracts key spatial and band information,
        and computes a reprojected extent in EPSG:4326 using an in-memory VRT. The metadata returned includes:
          - geo_transform: The affine transformation parameters from the original dataset.
          - x_size: The pixel width of the dataset.
          - y_size: The pixel height of the dataset.
          - projection: The projection string of the original dataset.
          - crs: The coordinate reference system in EPSG format (derived from the dataset's projection).
          - bands: A JSON string containing the band information retrieved from the dataset.
          - length_unit: The unit of measurement for the spatial reference (e.g., meter).

        Note:
            The function currently reprojects the dataset solely to extract the raster extent,
            which might be optimized in future revisions.

        Returns:
            dict: A dictionary containing the extracted metadata.
        """
        # Figure out aws options
        ds = gdal.Open(self.gdal_path)

        o = {
            "geo_transform": ds.GetGeoTransform(),
            "x_size": ds.RasterXSize,
            "y_size": ds.RasterXSize,
            "projection": ds.GetProjection(),
            "crs": "EPSG:"
            + osr.SpatialReference(ds.GetProjection()).GetAttrValue("AUTHORITY", 1),
            "bands": json.dumps(self.get_bands(ds)),
            "length_unit": ds.GetSpatialRef().GetAttrValue("UNIT"),
        }

        ds = None
        return o

    def get_extent(self):
        x_min = self.geo_transform[0]
        y_max = self.geo_transform[3]
        x_max = x_min + self.geo_transform[1] * self.x_size
        y_min = y_max + self.geo_transform[5] * self.y_size

        return (x_min, y_min, x_max, y_max)

    def get_wgs_extent(self):
        # Get the geotransform
        gt = self.geo_transform

        # Get raster dimensions
        cols = self.x_size
        rows = self.y_size

        # Get extent in original projection
        ext = []
        xarr = [0, cols]
        yarr = [0, rows]

        # Get the coordinates of all corners
        for px in xarr:
            for py in yarr:
                x = gt[0] + (px * gt[1]) + (py * gt[2])
                y = gt[3] + (px * gt[4]) + (py * gt[5])
                ext.append([x, y])

        # Get source coordinate system
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(self.projection)

        # Get target coordinate system (WGS84)
        tgt_srs = osr.SpatialReference()
        tgt_srs.ImportFromEPSG(4326)
        tgt_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        # Create transform
        transform = osr.CoordinateTransformation(src_srs, tgt_srs)

        # Transform all corners to WGS84
        wgs84_ext = []
        for x, y in ext:
            try:
                point = transform.TransformPoint(x, y)
                wgs84_ext.append([point[0], point[1]])
            except Exception as e:
                print(f"Warning: Transformation failed: {e}")

        wgs84_ext = np.array(wgs84_ext)

        # Get min/max coordinates
        bbox = [
            np.min(wgs84_ext[:, 0]).item(),  # minx
            np.min(wgs84_ext[:, 1]).item(),  # miny
            np.max(wgs84_ext[:, 0]).item(),  # maxx
            np.max(wgs84_ext[:, 1]).item(),  # maxy
        ]

        return bbox

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
