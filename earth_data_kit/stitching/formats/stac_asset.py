from earth_data_kit.utilities import helpers
from osgeo import gdal, osr
import pystac
import concurrent.futures
import earth_data_kit.utilities as utilities
from tqdm import tqdm
from earth_data_kit.stitching.classes.tile import Tile
import logging
import json
import pandas as pd
from earth_data_kit.stitching.engines.stac import STAC
import concurrent.futures
import earth_data_kit.utilities.geo as geo
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension
from pystac.extensions.eo import EOExtension

logger = logging.getLogger(__name__)


class STACAssetAdapter:
    def __init__(self) -> None:
        self.name = "STAC Asset"

    @staticmethod
    def get_bands_from_asset(item, asset, key, gdal_path):
        bands = []
        # Checking raster_ext
        if RasterExtension.has_extension(item):
            raster_ext = asset.ext.raster

            for idx in range(len(raster_ext.bands)):
                bands.append({
                    "nodataval": raster_ext.bands[idx].nodata,
                    "dtype": raster_ext.bands[idx].data_type,
                    "source_idx": idx + 1,
                    "description": key,
                })

        if len(bands) > 0:
            logger.info("Found in raster_ext")
            return bands

        ds = gdal.OpenEx(gdal_path, gdal.OF_READONLY)
        bands = geo._get_bands(ds)
        
        for idx in range(len(bands)):
            bands[idx]["description"] = key

        # Not using eo_ext as of now.

        return bands

    @staticmethod
    def get_projection_info(item, asset):
        if ProjectionExtension.has_extension(item):
            proj_ext = asset.ext.proj
        else:
            return None

        srs = osr.SpatialReference()

        if proj_ext.wkt2:  # WKT2 string available
            srs.ImportFromWkt(proj_ext.wkt2)
        elif proj_ext.epsg:  # EPSG code
            srs.ImportFromEPSG(proj_ext.epsg)

        # Convert proj_ext.transform (STAC: [a, b, d, e, x, y]) to GDAL geotransform (GDAL: [x, a, b, y, d, e])
        # STAC: [scale_x, shear_x, shear_y, scale_y, translate_x, translate_y]
        # GDAL: [origin_x, pixel_width, rotation_x, origin_y, rotation_y, pixel_height]
        # STAC order: [a, b, d, e, x, y]
        # GDAL order: [x, a, b, y, d, e]
        stac_transform = proj_ext.transform

        # Check if transform exists and handle None case
        if stac_transform is None:
            raise ValueError("STAC transform is None - cannot process asset without projection info")
        
        # Sometimes stack gives transform as 9 values - [a, b, d, e, x, y, 0, 0, 1]
        if len(stac_transform) == 9:
            stac_transform = stac_transform[:6]
            

        x_res, x_rot, x_ul, y_rot, y_res, y_ul = tuple(stac_transform)
        gdal_transform = (x_ul, x_res, x_rot, y_ul, y_rot, y_res)
        metadata = {
            "geo_transform": gdal_transform,
            "projection": srs.ExportToWkt(),
            "x_size": proj_ext.shape[0],
            "y_size": proj_ext.shape[1],
            "crs": f"EPSG:{proj_ext.epsg}",
            "length_unit": srs.GetAttrValue("UNIT"),
        }
        return metadata

    @staticmethod
    def get_metadata_from_stac_asset(item, asset, key, gdal_path):
        # geo_transform
        # projection
        # x_size
        # y_size
        # crs
        # length_unit
        # bands
        # band.source_idx
        # band.description
        # band.dtype
        # band.nodataval
        # Helper to get extension from asset, then item
        metadata = STACAssetAdapter.get_projection_info(item, asset)
        if metadata is None:
            return None

        # Here the metadata contains pretty much everything except proper band information
        # So we use stac's raster and eo extensions to get band information
        bands = STACAssetAdapter.get_bands_from_asset(item, asset, key, gdal_path)
        metadata["bands"] = bands

        return metadata

    @staticmethod
    def is_asset_allowed(asset):
        # Check that asset has roles and that "data" is in roles
        if not hasattr(asset, "roles") or asset.roles is None:
            return False
        # Check if media_type starts with any of the allowed media types
        allowed_media_types = ["image/jp2", "image/tiff"]
        return any(
            asset.media_type.startswith(allowed) for allowed in allowed_media_types
        )

    @staticmethod
    def to_vsi(url):
        if url.startswith("s3://"):
            return url.replace("s3://", "/vsis3/")
        elif url.startswith("https://"):
            return f"/vsicurl/{url}"
        else:
            raise ValueError(
                f"Unknown protocol found in asset href: {url}. "
                "Please raise an issue at https://github.com/earth-data-kit/earth-data-kit/issues with details about the STAC asset."
            )

    def _get_assets_and_metadata(self, df):
        rows = []

        def process_asset(args):
            df_row, key, asset = args
            asset_row = [df_row.date, df_row.tile_name, df_row.engine_path]
            try:
                # Construct GDAL path from asset href
                if df_row.engine_path.startswith("https://planetarycomputer.microsoft.com/api/stac/v1/collections/"):
                    _, collection_name = STAC._parse_stac_url(df_row.engine_path)
                    gdal_path = f"/vsicurl?pc_url_signing=yes&pc_collection={collection_name}&url={asset.href}"
                else:
                    gdal_path = STACAssetAdapter.to_vsi(asset.href)
                asset_row.append(gdal_path)

                if not STACAssetAdapter.is_asset_allowed(asset):
                    return None

                # Finally getting metadata
                item = pystac.Item.from_file(df_row.engine_path)
                metadata = STACAssetAdapter.get_metadata_from_stac_asset(
                    item, asset, key, gdal_path
                )
                if metadata is None:
                    return None

                asset_row.append(metadata["geo_transform"])
                asset_row.append(metadata["projection"])
                asset_row.append(metadata["x_size"])
                asset_row.append(metadata["y_size"])
                asset_row.append(metadata["crs"])
                asset_row.append(metadata["length_unit"])
                asset_row.append(metadata["bands"])
                return asset_row
            except Exception as e:
                logger.exception(e)
                return None

        # Prepare all (df_row, key, asset) tuples to process
        asset_args = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            future_to_row = {
                executor.submit(pystac.Item.from_file, df_row.engine_path): df_row
                for df_row in df.itertuples()
            }
            for future in tqdm(
                future_to_row,
                total=len(future_to_row),
                desc="Reading STAC items",
            ):
                df_row = future_to_row[future]
                try:
                    item = future.result()
                    for key, asset in item.assets.items():
                        # Filter assets before processing to avoid errors
                        if STACAssetAdapter.is_asset_allowed(asset):
                            asset_args.append((df_row, key, asset))
                except Exception as e:
                    logger.error(
                        f"Error loading STAC item for {df_row.engine_path}: {e}"
                    )

        rows = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            results = list(
                tqdm(
                    executor.map(process_asset, asset_args),
                    total=len(asset_args),
                    desc="Processing STAC assets",
                )
            )
            for asset_row in results:
                if asset_row is not None:
                    rows.append(asset_row)

        _df = pd.DataFrame(
            rows,
            columns=pd.Index(
                [
                    "date",
                    "tile_name",
                    "engine_path",
                    "gdal_path",
                    "geo_transform",
                    "projection",
                    "x_size",
                    "y_size",
                    "crs",
                    "length_unit",
                    "bands",
                ]
            ),
        )
        _df = _df[_df["geo_transform"].notna()].reset_index(drop=True)
        return _df

    def create_tiles(self, scan_df, band_locator=None):
        df = self._get_assets_and_metadata(scan_df)
        tiles = Tile.from_df(df)
        return tiles