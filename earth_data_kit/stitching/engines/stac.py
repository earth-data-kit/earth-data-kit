from pystac_client import Client
from pystac.extensions.eo import EOExtension
from datetime import datetime
import shapely.geometry
from earth_data_kit.stitching.classes.tile import Tile
import logging
from osgeo import gdal
import os
import earth_data_kit.stitching.engines.commons as commons
import pandas as pd

logger = logging.getLogger(__name__)


class STAC:
    def __init__(self) -> None:
        self.name = "stac"
        pass

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        _source = source.rstrip('/')
        
        if '/collections/' in source:
            parts = _source.split('/collections/')
            if len(parts) == 2:
                catalog_url = parts[0]
                collection_name = parts[1]
                return catalog_url, collection_name
        
        return _source, None

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        catalog_url, collection_name = self._parse_stac_url(source)
        
        if collection_name is None:
            # URL is stac catalog, raise an error for no collection name
            raise ValueError("Collection name is required for STAC catalog")
        

        # Open STAC catalog
        catalog = Client.open(catalog_url)
        
        # Prepare search parameters
        search_kwargs = {}
        
        search_kwargs["collections"] = [collection_name]
        # Add time filter if provided
        if time_opts and 'start' in time_opts and 'end' in time_opts:
            search_kwargs['datetime'] = [time_opts['start'], time_opts['end']]
        
        # Add spatial filter if provided
        if space_opts and 'bbox' in space_opts:
            bbox = space_opts['bbox']
            search_kwargs['bbox'] = bbox
        
        # Search for items using the collection
        results = catalog.search(**search_kwargs)
        tiles = []

        # row.engine_path,
        # row.gdal_path,
        # row.date,
        # row.tile_name,
        # row.geo_transform,
        # row.projection,
        # row.bands,
        # row.length_unit,
        # row.x_size,
        # row.y_size,
        # row.crs,

        # Need 4 columns date, tile_name, engine_path, gdal_path
        assets = []
        # Process each item/tile
        for item in results.items():
            if item.ext.has("eo"):
                bands = item.ext.eo.bands
                print (bands, len(bands))
                for band in bands:
                    print (band.keys())
                return

            # Get the STAC item URL
            for _, asset in item.assets.items():
                if asset.media_type.startswith('image/'):
                    asset_row = []
                    asset_row.append(item.datetime)
                    asset_row.append(item.id)
                    asset_row.append(asset.href)
                    if asset.href.startswith('s3://'):
                        asset_row.append(asset.href.replace('s3://', '/vsis3/'))
                    else:
                        asset_row.append(asset.href)
                    assets.append(asset_row)

        df = pd.DataFrame(assets, columns=["date", "tile_name", "engine_path", "gdal_path"])
        

        metadata = commons.get_tiles_metadata(
            df["gdal_path"].tolist(), band_locator
        )
        for idx in range(len(metadata)):
            if metadata[idx] is None:
                continue
            df.at[idx, "geo_transform"] = metadata[idx]["geo_transform"]
            df.at[idx, "projection"] = metadata[idx]["projection"]
            df.at[idx, "x_size"] = metadata[idx]["x_size"]
            df.at[idx, "y_size"] = metadata[idx]["y_size"]
            df.at[idx, "crs"] = metadata[idx]["crs"]
            df.at[idx, "length_unit"] = metadata[idx]["length_unit"]
            # Passing array of jsons in a dataframe "bands" column
            df.at[idx, "bands"] = metadata[idx]["bands"]
        df = df[df["geo_transform"].notna()].reset_index(drop=True)
        print (df)
        return
        tiles = Tile.from_df(df)
        return tiles

    def sync(self):
        pass
