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
import concurrent.futures

logger = logging.getLogger(__name__)


class STACAssetAdapter:
    def __init__(self) -> None:
        self.name = "STAC Asset"

    @staticmethod
    def get_bands_from_asset(raster_ext, eo_ext, key):
        bands = []

        # Check if raster_ext is None or if raster_ext.bands is None or empty
        if raster_ext is None or raster_ext.bands is None:
            return bands

        idx = 0
        for band in raster_ext.bands:
            # Try to get band name from eo extension if available
            description = key
            if eo_ext is not None:
                try:
                    description = eo_ext.bands[idx].name
                except Exception:
                    description = key

            o = {
                "nodataval": band.nodata,
                "dtype": band.data_type,
                "source_idx": idx + 1,
                "description": description,
            }
            idx = idx + 1
            bands.append(o)

        return bands

    @staticmethod
    def get_projection_info(proj_ext):
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

    def get_metadata_from_stac_asset(item, asset, key):
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
        raster_ext = None
        try:
            raster_ext = asset.ext.raster or item.ext.raster
        except Exception:
            pass  # raster extension not available

        # Try to get eo extension, but it may not exist
        eo_ext = None
        try:
            eo_ext = asset.ext.eo or item.ext.eo
        except Exception:
            pass  # eo extension not available

        proj_ext = None
        try:
            proj_ext = asset.ext.proj or item.ext.proj
        except Exception:
            pass  # proj extension not available

        metadata = STACAssetAdapter.get_projection_info(proj_ext)

        # Here the metadata contains pretty much everything except proper band information
        # So we use stac's raster and eo extensions to get band information
        bands = STACAssetAdapter.get_bands_from_asset(raster_ext, eo_ext, key)
        metadata["bands"] = bands

        return metadata

    @staticmethod
    def is_asset_allowed(asset):
        # Check that asset has roles and that "data" is in roles
        if not hasattr(asset, "roles") or asset.roles is None:
            return False
        if "data" not in asset.roles:
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
                gdal_path = STACAssetAdapter.to_vsi(asset.href)
                asset_row.append(gdal_path)

                drv = gdal.OpenEx(gdal_path, gdal.OF_READONLY)
                if drv is None:
                    return None

                if not STACAssetAdapter.is_asset_allowed(asset):
                    return None

                # Finally getting metadata
                item = pystac.Item.from_file(df_row.engine_path)
                metadata = STACAssetAdapter.get_metadata_from_stac_asset(
                    item, asset, key
                )
                if metadata is None:
                    return None

                # If bands list is empty (because raster extension was missing or had None bands),
                # extract band information directly from GDAL
                if not metadata["bands"]:
                    bands = []
                    for band_idx in range(1, drv.RasterCount + 1):
                        band = drv.GetRasterBand(band_idx)
                        nodata = band.GetNoDataValue()

                        # Map GDAL data type to string
                        gdal_type = band.DataType
                        dtype_mapping = {
                            gdal.GDT_Byte: "uint8",
                            gdal.GDT_UInt16: "uint16",
                            gdal.GDT_Int16: "int16",
                            gdal.GDT_UInt32: "uint32",
                            gdal.GDT_Int32: "int32",
                            gdal.GDT_Float32: "float32",
                            gdal.GDT_Float64: "float64",
                        }
                        data_type = dtype_mapping.get(gdal_type, "uint16")

                        bands.append({
                            "nodataval": nodata,
                            "dtype": data_type,
                            "source_idx": band_idx,
                            "description": key,  # Use asset key as description
                        })
                    metadata["bands"] = bands

                asset_row.append(metadata["geo_transform"])
                asset_row.append(metadata["projection"])
                asset_row.append(metadata["x_size"])
                asset_row.append(metadata["y_size"])
                asset_row.append(metadata["crs"])
                asset_row.append(metadata["length_unit"])
                asset_row.append(metadata["bands"])
                return asset_row
            except Exception as e:
                logger.error(e)
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
                concurrent.futures.as_completed(future_to_row),
                total=len(future_to_row),
                desc="Preparing STAC asset jobs",
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
            columns=[
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
            ],
        )
        _df = _df[_df["geo_transform"].notna()].reset_index(drop=True)
        return _df

    def create_tiles(self, scan_df, band_locator=None):
        df = self._get_assets_and_metadata(scan_df)
        tiles = Tile.from_df(df)
        return tiles