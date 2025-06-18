from pystac_client import Client
from pystac.extensions.eo import EOExtension
from datetime import datetime
import shapely.geometry
from earth_data_kit.stitching.classes.tile import Tile
import logging
from osgeo import gdal
import os

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
        logger.info(f"Scanning STAC source: {source}")
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

        item_urls = []
        # Process each item/tile
        for item in results.items():
            item_urls.append(os.path.join(catalog_url, 'collections', collection_name, 'items', item.id))
            print (item_urls)
            return
        
        print (item_urls)
        return tiles

    def sync(self):
        pass
