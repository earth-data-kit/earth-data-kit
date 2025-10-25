from pystac_client import Client
from urllib.parse import urlparse
import logging
import json
from osgeo import gdal
import os
import pandas as pd
import concurrent.futures
import earth_data_kit.utilities.helpers as helpers

logger = logging.getLogger(__name__)


class STAC:
    def __init__(self) -> None:
        self.name = "stac"

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        _source = source.rstrip("/")

        if "/collections/" in source:
            parts = _source.split("/collections/")
            if len(parts) == 2:
                catalog_url = parts[0]
                collection_name = parts[1]
                return catalog_url, collection_name

        return _source, None

    def _search_catalog(self, catalog_url, collection_name, time_opts, space_opts):
        """
        Helper method to search STAC catalog with filters.

        Args:
            catalog_url: STAC catalog URL
            collection_name: Collection name to search
            time_opts: Time filter options
            space_opts: Spatial filter options

        Returns:
            Search results iterator
        """
        # Open STAC catalog
        logger.info(f"Opening STAC catalog: {catalog_url}")
        catalog = Client.open(catalog_url)

        # Prepare search parameters
        search_kwargs = {"collections": [collection_name]}

        # Add time filter if provided
        if time_opts and "start" in time_opts and "end" in time_opts:
            search_kwargs["datetime"] = [time_opts["start"], time_opts["end"]]
            logger.info(f"Time filter: {time_opts['start']} to {time_opts['end']}")

        # Add spatial filter if provided
        if space_opts and "bbox" in space_opts:
            bbox = space_opts["bbox"]
            search_kwargs["bbox"] = bbox
            logger.info(f"Spatial filter (bbox): {bbox}")

        # Search for items using the collection
        logger.info(f"Searching collection: {collection_name}")
        results = catalog.search(**search_kwargs) # type: ignore
        return results

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        
        catalog_url, collection_name = self._parse_stac_url(source)

        if collection_name is None:
            raise ValueError(
                "Collection name is required for STAC catalog. "
                "Please provide a URL like: https://catalog.com/collections/{collection_id}"
            )

        # Search catalog with filters
        results = self._search_catalog(catalog_url, collection_name, time_opts, space_opts)

        # Ensure tmp_path directory exists
        os.makedirs(tmp_path, exist_ok=True)

        items = []
        # Process each STAC item and extract assets
        for item in results.items():
            item_row = [item.datetime, item.id, item.self_href, item.self_href]
            items.append(item_row)

        df = pd.DataFrame(
            items, columns=pd.Index(["date", "tile_name", "engine_path", "gdal_path"])
        )

        logger.info(f"Found {len(df)} STAC items")
        return df

    def _sync_s3(self, source, dest):
        raise NotImplementedError("S3 assets syncing is not yet supported for STAC")

    def _sync_http(self, source, dest):
        # Will have /vsicurl/ in the beginning
        # Remove the /vsicurl/ prefix to get the actual URL
        url = source[len("/vsicurl/") :]
        # Ensure the destination directory exists
        dest_dir = os.path.dirname(dest)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
        # Download the file using wget
        os.system(f"wget -O '{dest}' '{url}'")
        return dest

    def sync(self, df, tmp_base_dir, overwrite=False):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            for band_tile in df.itertuples():
                local_path = None
                protocol = None
                if band_tile.tile.gdal_path.startswith("/vsicurl/"):
                    url = band_tile.tile.gdal_path
                    url_wo_gdal_prefix = url[len("/vsicurl/") :]
                    url_parts = urlparse(url_wo_gdal_prefix)
                    local_path = f"{tmp_base_dir}/raw-data{url_parts.path}"
                    protocol = "http"
                elif band_tile.tile.gdal_path.startswith("/vsis3/"):
                    protocol = "s3"
                    raise ValueError("S3 assets syncing is not yet supported for STAC")
                else:
                    raise ValueError(
                        f"Unknown protocol found in asset href: {band_tile.tile.gdal_path}. "
                        "Please raise an issue at https://github.com/earth-data-kit/earth-data-kit/issues with details about the STAC asset."
                    )
                try:
                    ds = gdal.Open(local_path)
                    # File exists and is valid, no need to sync, unless overwrite is True
                    if overwrite:
                        if protocol == "http":
                            executor.submit(
                                self._sync_http, band_tile.tile.gdal_path, local_path
                            )
                        elif protocol == "s3":
                            executor.submit(
                                self._sync_s3, band_tile.tile.gdal_path, local_path
                            )
                    else:
                        logger.info("Tile found, not overwriting")
                except Exception as e:
                    # Error getting metadata, file will be synced
                    if protocol == "http":
                        executor.submit(
                            self._sync_http, band_tile.tile.gdal_path, local_path
                        )
                    elif protocol == "s3":
                        executor.submit(
                            self._sync_s3, band_tile.tile.gdal_path, local_path
                        )
                # Updating the file path to local_path
                band_tile.tile.gdal_path = local_path

            executor.shutdown(wait=True)

        return df

