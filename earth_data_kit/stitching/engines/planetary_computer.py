import logging
import pandas as pd
import shapely.geometry
import planetary_computer as pc
from pystac_client import Client
from earth_data_kit.stitching.engines.commons import get_tiles_metadata, aggregate_temporally
from earth_data_kit.stitching.classes.tile import Tile
import os
from urllib.parse import urlparse
import concurrent.futures
from osgeo import gdal
import earth_data_kit.utilities.helpers as helpers
import subprocess
import re
from earth_data_kit.utilities.geo import NonRetryableException

logger = logging.getLogger(__name__)


class PlanetaryComputer:
    def __init__(self) -> None:
        self.name = "planetary_computer"

    def is_media_type_allowed(self, media_type):
        allowed_media_types = ["image/jp2", "image/tiff"]
        return any(media_type.startswith(allowed) for allowed in allowed_media_types)

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        _source = source.rstrip("/")
        if "/collections/" in source:
            parts = _source.split("/collections/")
            if len(parts) == 2:
                return parts[0], parts[1]
        return _source, None

    """def get_stac_band_names(self, catalog_url: str, collection_name: str) -> list[dict]:
        try:
            catalog = Client.open(catalog_url)
            collection = catalog.get_collection(collection_name)
            
            band_names = []
            assets = collection.extra_fields.get("item_assets", {})
            
            for name, meta in assets.items():
                roles = meta.get("roles", [])
                if any(role.lower() == "data" for role in roles):
                    band_names.append(name)
            
            return [
                {
                    "source_idx": idx + 1,
                    "description": name,
                    "dtype": "Unknown",  
                    "nodataval": None,   
                }
                for idx, name in enumerate(band_names)
            ]
            
        except Exception as e:
            logger.error(f"Failed to get STAC band names from {catalog_url}/{collection_name}: {e}")
            raise NonRetryableException(f"STAC metadata retrieval failed: {e}")"""

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        catalog_url, collection_name = self._parse_stac_url(source)
        if collection_name is None:
            raise ValueError("Collection name is required for Planetary Computer STAC")

        catalog = Client.open(catalog_url)
        search_kwargs = {"collections": [collection_name]}
        if time_opts and "start" in time_opts and "end" in time_opts:
            search_kwargs["datetime"] = [time_opts["start"], time_opts["end"]]
        if space_opts and "bbox" in space_opts:
            search_kwargs["bbox"] = space_opts["bbox"]

        results = catalog.search(**search_kwargs)
        assets = []

        for item in results.items():
            signed_item = pc.sign(item)
            original_assets = item.assets
            for key, asset in signed_item.assets.items():
                if self.is_media_type_allowed(asset.media_type):
                    original_href = original_assets[key].href
                    signed_href = asset.href
                    gdal_path = f"/vsicurl/{signed_href}" if signed_href.startswith("https://") else None
                    if gdal_path is None:
                        raise ValueError(f"Unsupported asset protocol: {original_href}. Only HTTPS supported.")
                    assets.append([
                        item.datetime,
                        item.id,
                        original_href,
                        gdal_path,
                        key  
                    ])

        df = pd.DataFrame(assets, columns=["date", "tile_name", "engine_path", "gdal_path", "band_name"])
        print(f"Discovered {len(df)} assets from Planetary Computer STAC")
        
        if band_locator == "stac":
            logger.info(f"Using STAC collection metadata for band discovery: {collection_name}")
            
            # Ensure files are local and readable
            df = self.sync(df, tmp_path, overwrite=False)
            
            # Get metadata from local files for spatial info and dtype
            metadata = []
            for idx, row in df.iterrows():
                try:
                    # Pass the STAC asset key to get_metadata for proper band naming
                 from earth_data_kit.utilities.geo import get_metadata
                 meta = get_metadata(row["gdal_path"], band_locator, stac_asset_key=row["band_name"])
                 metadata.append(meta)
                except Exception as e:
                    logger.error(f"Failed to get metadata for {row['gdal_path']}: {e}")
                    metadata.append(None)
            df["geo_transform"] = None
            df["projection"] = None
            df["x_size"] = None
            df["y_size"] = None
            df["crs"] = None
            df["length_unit"] = None
            df["bands"] = None

            for idx in range(len(metadata)):
                if isinstance(metadata[idx], dict):
                    df.at[idx, "geo_transform"] = metadata[idx]["geo_transform"]
                    df.at[idx, "projection"] = metadata[idx]["projection"]
                    df.at[idx, "x_size"] = metadata[idx]["x_size"]
                    df.at[idx, "y_size"] = metadata[idx]["y_size"]
                    df.at[idx, "crs"] = metadata[idx]["crs"]
                    df.at[idx, "length_unit"] = metadata[idx]["length_unit"]
                    df.at[idx, "bands"] = metadata[idx]["bands"]

        else:
            df = self.sync(df, tmp_path, overwrite=False)
        
            metadata = get_tiles_metadata(df["gdal_path"].tolist(), band_locator)
            df["geo_transform"] = None
            df["projection"] = None
            df["x_size"] = None
            df["y_size"] = None
            df["crs"] = None
            df["length_unit"] = None
            df["bands"] = None

            for idx in range(len(metadata)):
                if isinstance(metadata[idx], dict):
                    df.at[idx, "geo_transform"] = metadata[idx]["geo_transform"]
                    df.at[idx, "projection"] = metadata[idx]["projection"]
                    df.at[idx, "x_size"] = metadata[idx]["x_size"]
                    df.at[idx, "y_size"] = metadata[idx]["y_size"]
                    df.at[idx, "crs"] = metadata[idx]["crs"]
                    df.at[idx, "length_unit"] = metadata[idx]["length_unit"]
                    df.at[idx, "bands"] = metadata[idx]["bands"]

        df = df[df["geo_transform"].notna()].reset_index(drop=True)

        if time_opts and "resolution" in time_opts and time_opts["resolution"] is not None:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if df["date"].dt.tz is None:
                df["date"] = df["date"].dt.tz_localize("UTC")
            else:
                df["date"] = df["date"].dt.tz_convert("UTC")
            df = aggregate_temporally(
                df,
                pd.to_datetime(time_opts["start"]),
                pd.to_datetime(time_opts["end"]),
                time_opts["resolution"],
            )

        tiles = Tile.from_df(df)
        return tiles

    def _sync_http(self, source, dest):
        """Download file from HTTPS URL to local destination using wget"""
        url = source[len("/vsicurl/"):]
        dest_dir = os.path.dirname(dest)
        os.makedirs(dest_dir, exist_ok=True)

        logger.info(f"Downloading {url} -> {dest}")
        try:
            subprocess.run(["wget", "-q", "-O", dest, url], check=True, timeout=300)
            if not os.path.exists(dest) or os.path.getsize(dest) < 1024:
                raise IOError(f"Downloaded file {dest} is empty or too small.")

            ds = gdal.Open(dest)
            if ds is None:
                raise IOError(f"Downloaded file {dest} cannot be opened by GDAL.")
            ds = None
        except subprocess.CalledProcessError as e:
            logger.error(f"wget failed for {url}: {e}")
            if os.path.exists(dest): os.remove(dest)
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            if os.path.exists(dest): os.remove(dest)
            raise

        return dest

    def sync(self, df, tmp_base_dir, overwrite=False):
        """Enhanced sync with better error handling and progress tracking"""
        futures = []
        download_count = 0
        skip_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            for band_tile in df.itertuples():
                if band_tile.gdal_path.startswith("/vsicurl/"):
                    url = band_tile.gdal_path
                    url_wo_prefix = url[len("/vsicurl/"):]
                    url_parts = urlparse(url_wo_prefix)
                    local_path = f"{tmp_base_dir}/raw-data{url_parts.path}"

                    file_exists = False
                    if os.path.exists(local_path) and not overwrite:
                        try:
                            ds = gdal.Open(local_path)
                            if ds is not None:
                                file_exists = True
                                ds = None
                                skip_count += 1
                        except Exception as e:
                            logger.warning(f"Could not open existing file {local_path}: {e}")

                    if not file_exists or overwrite:
                        futures.append(
                            executor.submit(self._sync_http, band_tile.gdal_path, local_path)
                        )
                        download_count += 1
                    else:
                        logger.info(f"Tile found at {local_path}, not overwriting")

                    # Update DataFrame directly instead of `band_tile.tile`
                    df.at[band_tile.Index, "gdal_path"] = local_path

                # Already local file
                elif band_tile.gdal_path.startswith("/app/data/") or os.path.exists(band_tile.gdal_path):
                    logger.info(f"Using existing local file: {band_tile.gdal_path}")

                else:
                    raise ValueError(
                        f"Unsupported protocol: {band_tile.gdal_path}. "
                        "Expected either /vsicurl/ (for download) or local file path."
                    )


            # Wait for downloads with progress tracking
            if futures:
                logger.info(f"Starting downloads: {download_count} new, {skip_count} existing")
                completed = 0
                for f in concurrent.futures.as_completed(futures):
                    try:
                        f.result()
                        completed += 1
                        if completed % 5 == 0 or completed == len(futures):
                            logger.info(f"Download progress: {completed}/{len(futures)} completed")
                    except Exception as e:
                        logger.error(f"Download failed: {e}")
                        raise
            else:
                logger.info("No downloads needed - all files are local")

        logger.info(f"Sync completed: {download_count} downloaded, {skip_count} skipped, {len(df) - download_count - skip_count} already local")
        return df