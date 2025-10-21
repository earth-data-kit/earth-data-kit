import os
import logging
from earth_data_kit.stitching.engines.stac import STAC
import pandas as pd

logger = logging.getLogger(__name__)


def sign_url(item_or_url):
    """
    Sign URLs using Planetary Computer API.
    Simple signing using only the planetary_computer library.
    """
    try:
        import planetary_computer

        # Case 1: STAC item dict
        if isinstance(item_or_url, dict):
            signed_item = planetary_computer.sign(item_or_url)
            logger.debug("Signed STAC item")
            return signed_item

        # Case 2: URL string
        signed_url = planetary_computer.sign_url(item_or_url)
        logger.debug("Signed URL")
        return signed_url

    except ImportError:
        logger.error("planetary_computer library not available. Install with: pip install planetary-computer")
        return item_or_url
    except Exception as e:
        logger.error(f"Error signing: {e}")
        return item_or_url


class PlanetaryComputer:
    """
    Planetary Computer engine with URL signing capabilities.
    Uses composition to leverage STAC functionality.
    """

    def __init__(self) -> None:
        self.name = "planetary_computer"
        self._stac = STAC()  # Composition: use STAC as a helper

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        """Delegate to STAC's URL parsing."""
        return self._stac._parse_stac_url(source)

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        """
        Scan Planetary Computer STAC catalog for available items.
        Extends STAC scanning with URL signing for each asset.
        """
        catalog_url, collection_name = self._parse_stac_url(source)

        if collection_name is None:
            raise ValueError(
                "Collection name is required for Planetary Computer STAC catalog. "
                "Please provide a URL like: https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection_id}"
            )

        # Use STAC's helper method to search catalog
        results = self._stac._search_catalog(catalog_url, collection_name, time_opts, space_opts)

        items = []
        for item in results.items():
            # Loop through all assets (bands)
            for asset_key, asset in item.assets.items():
                # Filter assets by media type and roles
                allowed_media_types = ["image/tiff", "image/jp2"]

                if not any(asset.media_type and asset.media_type.startswith(media_type)
                          for media_type in allowed_media_types):
                    continue

                if not asset.roles or "data" not in asset.roles:
                    continue

                url = asset.href
                signed_url = sign_url(url)

                # Convert signed URL to GDAL-accessible path
                if isinstance(signed_url, str) and signed_url.startswith("http"):
                    gdal_path = f"/vsicurl/{signed_url}"
                else:
                    gdal_path = signed_url

                description = asset.title or asset.description or asset_key

                item_row = [
                    item.datetime,
                    f"{item.id}_{asset_key}",
                    item.self_href,
                    gdal_path,
                    description
                ]
                items.append(item_row)

        df = pd.DataFrame(
            items,
            columns=pd.Index(["date", "tile_name", "engine_path", "gdal_path", "description"])
        )

        logger.info(f"Found {len(df)} assets across all items")
        return df

    def sync(self, df, tmp_base_dir, overwrite=True):
        """
        Sync (download) files from Planetary Computer to local storage.
        Delegates to STAC's sync method since the logic is the same.
        """
        return self._stac.sync(df, tmp_base_dir, overwrite)