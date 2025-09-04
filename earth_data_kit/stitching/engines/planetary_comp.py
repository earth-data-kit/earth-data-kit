# planetary_comp.py
import logging
import pandas as pd
import shapely.geometry
import planetary_computer as pc
from pystac_client import Client
import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
import json
import os
from urllib.parse import urlparse
import concurrent.futures
from osgeo import gdal
import earth_data_kit as edk
import earth_data_kit.utilities.helpers as helpers
import logging


logger = logging.getLogger(__name__)


class PlanetaryComp:

    def __init__(self) -> None:
        self.name = "planetary_comp"

    def is_media_type_allowed(self, media_type):
        allowed_media_types = ["image/jp2", "image/tiff"]
        return any(media_type.startswith(allowed) for allowed in allowed_media_types)

    def _parse_stac_url(self, source: str):
        _source = source.rstrip("/")
        if "/collections/" in source:
            parts = _source.split("/collections/")
            if len(parts) == 2:
                catalog_url = parts[0]
                collection_name = parts[1]
                return catalog_url, collection_name
        return _source, None

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        catalog_url, collection_name = self._parse_stac_url(source)
        if collection_name is None:
            catalog_url = "https://planetarycomputer.microsoft.com/api/stac/v1"
            collection_name = source

        client = Client.open(catalog_url)

        # prepare search kwargs
        search_kwargs = {"collections": [collection_name]}

        # add datetime if provided
        if time_opts and "start" in time_opts and "end" in time_opts:
            search_kwargs["datetime"] = [time_opts["start"], time_opts["end"]]

        # add bbox if provided
        if space_opts and "bbox" in space_opts:
            search_kwargs["bbox"] = space_opts["bbox"]

        logger.info(f"PlanetaryComp: searching {catalog_url} collections {collection_name} with {search_kwargs}")

        results = client.search(**search_kwargs)

        assets = []
        for item in results.item_collection():
            for asset_key, asset in item.assets.items():
                media_type = getattr(asset, "media_type", None)
                if media_type is not None and not self.is_media_type_allowed(media_type):
                    continue
                engine_path = asset.href
                try:
                    signed = pc.sign(asset.href)
                    if isinstance(signed, dict):
                        signed_href = signed.get("href") or signed.get("url") or asset.href
                    else:
                        signed_href = signed
                except Exception:
                    signed_href = asset.href

                if signed_href.startswith("s3://"):
                    gdal_path = signed_href.replace("s3://", "/vsis3/")
                elif signed_href.startswith("https://") or signed_href.startswith("http://"):
                    gdal_path = f"/vsicurl/{signed_href}"
                else:
                    
                    gdal_path = signed_href

            
                date = item.datetime
                tile_name = item.id

                assets.append([date, tile_name, engine_path, gdal_path, asset_key])

        if not assets:
            logger.warning("PlanetaryComp: No assets found for given filters.")
            return []

        df = pd.DataFrame(assets, columns=["date", "tile_name", "engine_path", "gdal_path", "asset_key"])

        metadata = commons.get_tiles_metadata(df["gdal_path"].tolist(), band_locator)

        df["geo_transform"] = None
        df["projection"] = None
        df["x_size"] = None
        df["y_size"] = None
        df["crs"] = None
        df["length_unit"] = None
        df["bands"] = None

        for idx in range(len(metadata)):
            if metadata[idx] is None:
                continue
            if not isinstance(metadata[idx], dict):
                continue
            df.at[idx, "geo_transform"] = metadata[idx].get("geo_transform")
            df.at[idx, "projection"] = metadata[idx].get("projection")
            df.at[idx, "x_size"] = metadata[idx].get("x_size")
            df.at[idx, "y_size"] = metadata[idx].get("y_size")
            df.at[idx, "crs"] = metadata[idx].get("crs")
            df.at[idx, "length_unit"] = metadata[idx].get("length_unit")
            df.at[idx, "bands"] = metadata[idx].get("bands")

        df = df[df["geo_transform"].notna()].reset_index(drop=True)

        if "resolution" in (time_opts or {}) and time_opts.get("resolution") is not None:
            df["date"] = pd.to_datetime(df["date"], utc=True)

        tiles = Tile.from_df(df)

        return tiles
    
    def _sync_http(self, source, dest):

        dest_dir = os.path.dirname(dest)
        os.makedirs(dest_dir, exist_ok=True)
        cmd = f"wget -O '{dest}' '{source}'"
        logger.info(f"Downloading {source} -> {dest}")
        os.system(cmd)
        return dest

    def _sync_s3(self, source, dest):
        
        dest_dir = os.path.dirname(dest)
        os.makedirs(dest_dir, exist_ok=True)
        # Remove /vsis3/ prefix to get the real S3 path
        s3_path = source.replace("/vsis3/", "s3://")
        logger.info(f"Syncing S3 {s3_path} -> {dest}")
        cmd = f"{edk.S5CMD_PATH} cp {s3_path} {dest}"
        os.system(cmd)
        return dest

    def sync(self, df, tmp_base_dir, overwrite=False, max_workers=None):
        
        if max_workers is None:
            max_workers = helpers.get_threadpool_workers()

        os.makedirs(f"{tmp_base_dir}/raw-data", exist_ok=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for band_tile in df.itertuples():
                local_path = None
                protocol = None

                if band_tile.tile.gdal_path.startswith("/vsicurl/"):
                    url = band_tile.tile.gdal_path[len("/vsicurl/") :]
                    local_path = f"{tmp_base_dir}/raw-data{urlparse(url).path}"
                    protocol = "http"
                elif band_tile.tile.gdal_path.startswith("/vsis3/"):
                    url = band_tile.tile.gdal_path
                    local_path = f"{tmp_base_dir}/raw-data{url.replace('/vsis3/', '')}"
                    protocol = "s3"
                else:
                    logger.warning(f"Unknown protocol: {band_tile.tile.gdal_path}, skipping")
                    continue

                # Check if local file exists
                file_exists = os.path.exists(local_path)
                try:
                    if file_exists and not overwrite:
                        logger.info(f"Tile exists, skipping: {local_path}")
                    else:
                        if protocol == "http":
                            futures.append(executor.submit(self._sync_http, band_tile.tile.gdal_path, local_path))
                        elif protocol == "s3":
                            futures.append(executor.submit(self._sync_s3, band_tile.tile.gdal_path, local_path))
                except Exception as e:
                    logger.error(f"Error syncing {band_tile.tile.gdal_path}: {e}")

                # Update the gdal_path immediately
                band_tile.tile.gdal_path = local_path

            # Wait for all downloads to complete
            concurrent.futures.wait(futures)

        logger.info("All tiles synced successfully")
        return df
        
        
