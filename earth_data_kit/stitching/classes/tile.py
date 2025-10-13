from osgeo import gdal, osr
import logging
import pandas as pd
import earth_data_kit.stitching.decorators as decorators
import json
import uuid
import numpy as np
import earth_data_kit.utilities as utilities

gdal.UseExceptions()

logger = logging.getLogger(__name__)


class Tile:
    def __init__(
        self,
        engine_path,
        gdal_path,
        date,
        tile_name,
        geo_transform,
        projection,
        bands,
        length_unit,
        x_size,
        y_size,
        crs,
    ) -> None:
        self.engine_path = engine_path
        self.gdal_path = gdal_path
        self.date = date
        self.tile_name = tile_name

        self.geo_transform = geo_transform
        self.projection = projection
        # Should be array of jsons
        self.bands = bands
        self.length_unit = length_unit
        self.x_size = x_size
        self.y_size = y_size
        self.crs = crs

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
                row.geo_transform,
                row.projection,
                row.bands,
                row.length_unit,
                row.x_size,
                row.y_size,
                row.crs,
            )
            tiles.append(tile)

        return tiles

    def get_extent(self):
        x_min = self.geo_transform[0]
        y_max = self.geo_transform[3]
        x_max = x_min + self.geo_transform[1] * self.x_size
        y_min = y_max + self.geo_transform[5] * self.y_size

        return (x_min, y_min, x_max, y_max)

    def get_wgs84_bbox_via_warp(self):
        """
        Return (west, south, east, north) in EPSG:4326 by warping to an in-memory VRT.
        Uses only gdal.Warp (fallback approach).
        """
        src = gdal.Open(self.gdal_path)
        if src is None:
            raise RuntimeError(f"Cannot open: {self.gdal_path}")

        # Create an in-memory VRT in WGS84
        vrt = gdal.Warp(
            destNameOrDestDS="",
            srcDSOrSrcDSTab=src,
            dstSRS="EPSG:4326",
            format="VRT",
            multithread=True,
        )
        if vrt is None:
            raise RuntimeError("gdal.Warp failed to create VRT in EPSG:4326.")

        w, h = vrt.RasterXSize, vrt.RasterYSize
        gt = vrt.GetGeoTransform()
        if gt is None:
            raise RuntimeError("Warped VRT has no geotransform.")

        # Compute bbox from the VRT geotransform (handles rotation if any)
        west = gt[0]
        east = gt[0] + gt[1] * w + gt[2] * h
        north = gt[3]
        south = gt[3] + gt[4] * w + gt[5] * h

        # Normalize to (minx, miny, maxx, maxy)
        west, east = (min(west, east), max(west, east))
        south, north = (min(south, north), max(south, north))

        # Clean up
        vrt = None
        src = None
        bbox = [west, south, east, north]
        return bbox

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
        src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        # Get target coordinate system (WGS84)
        tgt_srs = osr.SpatialReference()
        tgt_srs.ImportFromEPSG(4326)
        tgt_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        # Create transform
        transform = osr.CoordinateTransformation(src_srs, tgt_srs)

        # Transform all corners to WGS84
        use_fallback = False
        wgs84_ext = []
        for x, y in ext:
            try:
                point = transform.TransformPoint(x, y)
                if any(np.isinf(coord) for coord in point):
                    raise ValueError(
                        f"Transformed point has inf values. Will fallback to warp implementation"
                    )
                wgs84_ext.append([point[0], point[1]])
            except Exception as e:
                logger.warning(e)
                use_fallback = True
                break

        if use_fallback:
            return self.get_wgs84_bbox_via_warp()

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
        return tuple(round(coord, 6) for coord in (self.geo_transform[1], self.geo_transform[5]))
