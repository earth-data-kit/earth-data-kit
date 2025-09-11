import logging
import pandas as pd
import planetary_computer as pc
from pystac_client import Client
from earth_data_kit.stitching.engines.commons import get_tiles_metadata, aggregate_temporally
from earth_data_kit.stitching.classes.tile import Tile
from earth_data_kit.utilities.geo import get_band_names_with_pystac

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

    def sign_and_get_metadata(self, df, band_locator, catalog_url, collection_name):
        """
        For each row in df: re-sign the STAC item and asset (by asset_key) to produce
        signed URLs used for metadata extraction. Then call get_tiles_metadata.
        """
        catalog = Client.open(catalog_url)
        signed_urls = []

        for row in df.itertuples():
                search = catalog.search(collections=[collection_name], ids=[row.tile_name])
                items = list(search.items())
                if not items:
                    signed_urls.append(None)
                    continue

                signed_item = pc.sign(items[0])
                signed_asset = signed_item.assets.get(getattr(row, "asset_key", None))
                signed_urls.append(signed_asset.href if signed_asset else None)

        metadata = get_tiles_metadata(signed_urls, band_locator)

        df = df.copy()
        df["geo_transform"] = None
        df["projection"] = None
        df["x_size"] = None
        df["y_size"] = None
        df["crs"] = None
        df["length_unit"] = None
        df["bands"] = None

        for idx, md in enumerate(metadata):
            if isinstance(md, dict):
                df.at[idx, "geo_transform"] = md.get("geo_transform")
                df.at[idx, "projection"] = md.get("projection")
                df.at[idx, "x_size"] = md.get("x_size")
                df.at[idx, "y_size"] = md.get("y_size")
                df.at[idx, "crs"] = md.get("crs")
                df.at[idx, "length_unit"] = md.get("length_unit")
                df.at[idx, "bands"] = md.get("bands")

        return df

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        """
        Discover STAC items in a collection, keep original_href as gdal_path,
        use signed hrefs for engine_path, expand multi-band assets into rows.
        """
        catalog_url, collection_name = self._parse_stac_url(source)
        if collection_name is None:
            raise ValueError("Collection name is required for Planetary Computer STAC")

        catalog = Client.open(catalog_url)

        # ✅ Use helper from geo.py to get allowed asset keys
        allowed_asset_keys = get_band_names_with_pystac(collection_name, catalog_url)

        search_kwargs = {"collections": [collection_name]}
        if time_opts and "start" in time_opts and "end" in time_opts:
            search_kwargs["datetime"] = [time_opts["start"], time_opts["end"]]
        if space_opts and "bbox" in space_opts:
            search_kwargs["bbox"] = space_opts["bbox"]

        results = catalog.search(**search_kwargs)
        assets = []

        for item in results.items():
            signed_item = pc.sign(item)
            for asset_key, signed_asset in signed_item.assets.items():
                if asset_key not in allowed_asset_keys:
                    continue
                if not self.is_media_type_allowed(signed_asset.media_type or ""):
                    continue

                signed_href = signed_asset.href
                original_href = item.assets[asset_key].href

                asset_meta = item.assets[asset_key].extra_fields or {}
                bands_meta = asset_meta.get("bands", [])

                if bands_meta:
                    for b in bands_meta:
                        band_name = b.get("name") or asset_key
                        assets.append([
                            item.datetime,
                            item.id,
                            asset_key,
                            band_name,
                            signed_href,
                            original_href
                        ])
                else:
                    assets.append([
                        item.datetime,
                        item.id,
                        asset_key,
                        asset_key,
                        signed_href,
                        original_href
                    ])

        df = pd.DataFrame(assets, columns=[
            "date", "tile_name", "asset_key", "band_name", "engine_path", "gdal_path"
        ])
        logger.info(f"Discovered {len(df)} assets from Planetary Computer STAC")

        df = self.sign_and_get_metadata(df, band_locator, catalog_url, collection_name)
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

        return Tile.from_df(df)
