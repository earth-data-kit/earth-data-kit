import os
import logging
from pystac_client import Client
import pandas as pd
import concurrent.futures
from urllib.parse import urlparse
from osgeo import gdal
import earth_data_kit.utilities.helpers as helpers

logger = logging.getLogger(__name__)


def sign_url(item_or_url):
    # Case 1: STAC item dict
    if isinstance(item_or_url, dict):
        try:
            import planetary_computer
            signed_item = planetary_computer.sign(item_or_url)
            logger.debug("Signed STAC item using planetary_computer library")
            return signed_item
        except ImportError:
            logger.warning(
                "planetary_computer library not available. Install with: pip install planetary-computer"
            )
            return item_or_url
        except Exception as e:
            logger.warning(f"Error signing STAC item: {e}")
            return item_or_url

    # Case 2: Asset URL string
    url = item_or_url
    sas_token = os.getenv("EDK_AZURE_SAS_TOKEN")
    if sas_token:
        separator = "&" if "?" in url else "?"
        signed_url = f"{url}{separator}{sas_token}"
        logger.debug("Signed URL using EDK_AZURE_SAS_TOKEN")
        return signed_url

    # If no signing method is available, return the original URL
    logger.debug("No signing method available, returning original URL")
    return url


class PlanetaryComputer:
    def __init__(self) -> None:
        self.name = "planetary_computer"

    @staticmethod
    def sign_stac_items(scan_df, tmp_path):
        """
        Sign STAC items and save them locally with SAS tokens.

        Args:
            scan_df: DataFrame with 'engine_path' column containing STAC item URLs
            tmp_path: Temporary directory path for saving signed items

        Returns:
            DataFrame with 'engine_path' updated to point to signed local files
        """
        import pystac

        # Create directory for signed STAC items
        signed_items_dir = os.path.join(tmp_path, "signed_stac_items")
        os.makedirs(signed_items_dir, exist_ok=True)

        # Group by engine_path to avoid signing the same item multiple times
        unique_engine_paths = scan_df['engine_path'].unique()
        engine_path_mapping = {}

        for engine_path in unique_engine_paths:
            try:
                # Load and sign the STAC item
                item = pystac.Item.from_file(engine_path)
                signed_item = sign_url(item.to_dict())

                if signed_item != item.to_dict():
                    # Item was signed, save it locally
                    signed_item_obj = pystac.Item.from_dict(signed_item)
                    signed_item_path = os.path.join(signed_items_dir, f"{signed_item_obj.id}.json")
                    signed_item_obj.save_object(dest_href=signed_item_path)
                    engine_path_mapping[engine_path] = signed_item_path
                    logger.debug(f"Signed and saved item {signed_item_obj.id}")
                else:
                    # Signing failed or not available, keep original
                    engine_path_mapping[engine_path] = engine_path
            except Exception as e:
                logger.error(f"Error signing STAC item {engine_path}: {e}")
                engine_path_mapping[engine_path] = engine_path

        # Update engine_path to point to signed items
        scan_df = scan_df.copy()
        scan_df['engine_path'] = scan_df['engine_path'].map(engine_path_mapping)
        return scan_df

    @staticmethod
    def sign_vrt_files(vrt_datasets):
        """
        Sign URLs in VRT files for Planetary Computer data access.

        Args:
            vrt_datasets: List of VRT dataset info dicts with 'source' keys
        """
        from xml.etree import ElementTree as ET

        for vrt_info in vrt_datasets:
            vrt_path = vrt_info.get("source")
            if vrt_path:
                try:
                    # Read the VRT file
                    tree = ET.parse(vrt_path)
                    root = tree.getroot()

                    # Find all SourceFilename elements and sign their URLs
                    for source_elem in root.iter("SourceFilename"):
                        if source_elem.text:
                            source_elem.text = str(sign_url(source_elem.text))

                    # Write the modified VRT back
                    tree.write(vrt_path)
                except Exception as e:
                    logger.warning(f"Error signing VRT file {vrt_path}: {e}")

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        """Parse STAC URL to extract catalog URL and collection name."""
        _source = source.rstrip("/")

        if "/collections/" in source:
            parts = _source.split("/collections/")
            if len(parts) == 2:
                catalog_url = parts[0]
                collection_name = parts[1]
                return catalog_url, collection_name

        return _source, None

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        """
        Scan Planetary Computer STAC catalog for available items.

        User supplies a source which is a STAC collection URL copied from Planetary Computer.
        Most of the code is similar to STAC.scan() as Planetary Computer is a STAC catalog.
        This can run without authentication. We save the URLs and return to discover function.

        Args:
            source: STAC collection URL from Planetary Computer
            time_opts: Dictionary with 'start' and 'end' datetime filters
            space_opts: Dictionary with 'bbox' for spatial filtering
            tmp_path: Temporary directory path
            band_locator: Band locator for metadata extraction

        Returns:
            DataFrame with columns: date, tile_name, engine_path, gdal_path
        """
        catalog_url, collection_name = self._parse_stac_url(source)

        if collection_name is None:
            # URL is STAC catalog, raise an error for no collection name
            raise ValueError(
                "Collection name is required for Planetary Computer STAC catalog. "
                "Please provide a URL like: https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection_id}"
            )

        # Open STAC catalog
        logger.info(f"Opening Planetary Computer catalog: {catalog_url}")
        catalog = Client.open(catalog_url)

        # Prepare search parameters
        search_kwargs = {}
        search_kwargs["collections"] = [collection_name]

        # Add time filter if provided
        if time_opts and "start" in time_opts and "end" in time_opts:
            search_kwargs["datetime"] = [time_opts["start"], time_opts["end"]]
            logger.info(
                f"Time filter: {time_opts['start']} to {time_opts['end']}"
            )

        # Add spatial filter if provided
        if space_opts and "bbox" in space_opts:
            bbox = space_opts["bbox"]
            search_kwargs["bbox"] = bbox
            logger.info(f"Spatial filter (bbox): {bbox}")

        # Search for items using the collection
        logger.info(f"Searching collection: {collection_name}")
        results = catalog.search(**search_kwargs)

        # Need 4 columns: date, tile_name, engine_path, gdal_path
        items = []

        for item in results.items():
            # Loop through all assets (bands)
            for asset_key, asset in item.assets.items():
                # Filter assets by media type and roles
                # Only allow GeoTIFF and JP2000 data assets
                allowed_media_types = ["image/tiff", "image/jp2"]

                # Check if media_type starts with any allowed type
                if not any(asset.media_type and asset.media_type.startswith(media_type)
                          for media_type in allowed_media_types):
                    continue

                # Check that asset has "data" role
                if not asset.roles or "data" not in asset.roles:
                    continue

                url = asset.href

                # Convert URL to GDAL-accessible path
                if isinstance(url, str) and url.startswith("http"):
                    gdal_path = f"/vsicurl/{url}"
                else:
                    gdal_path = url

                description = asset.title or asset.description or asset_key

                item_row = [
                    item.datetime,
                    f"{item.id}_{asset_key}",  # unique tile_name per asset
                    item.self_href,            # engine path
                    gdal_path,                 # GDAL-accessible path (unsigned)
                    description                # band description
                ]
                items.append(item_row)

        # Convert to DataFrame
        df = pd.DataFrame(
            items,
            columns=pd.Index(["date", "tile_name", "engine_path", "gdal_path", "description"])
        )

        return df
    
   



    
    

    