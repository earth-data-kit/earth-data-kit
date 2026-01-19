import logging
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import zipfile

logger = logging.getLogger(__name__)

class Bhoonidhi:
    """
    Bhoonidhi engine for ISRO's earth observation satellite data.
    Provides authentication and STAC-compliant search.
    """
    BASE_URL = "https://bhoonidhi-api.nrsc.gov.in"
    AUTH_URL = f"{BASE_URL}/auth/token"
    STAC_URL = f"{BASE_URL}/data"
    DOWNLOAD_URL = f"{BASE_URL}/download"
    TOKEN_FILE = os.path.expanduser("~/.bhoonidhi_token")

    def __init__(self, user_id=None, password=None):
        self.name = "bhoonidhi"
        self.user_id = user_id or os.getenv("BHOONIDHI_USERNAME")
        self.password = password or os.getenv("BHOONIDHI_PASSWORD")
        self.access_token = None
        self.token_expires_at = None
        self._load_cached_token()

    def _load_cached_token(self):
        try:
            if os.path.exists(self.TOKEN_FILE):
                mtime = datetime.fromtimestamp(os.path.getmtime(self.TOKEN_FILE))
                if datetime.now() - mtime < timedelta(minutes=18):
                    with open(self.TOKEN_FILE, 'r') as f:
                        self.access_token = f.read().strip()
                    self.token_expires_at = mtime + timedelta(minutes=20)
                    logger.info("Loaded cached Bhoonidhi token")
        except Exception:
            pass

    def _save_token(self, token):
        try:
            with open(self.TOKEN_FILE, 'w') as f:
                f.write(token)
        except Exception:
            pass

    def authenticate(self, force_refresh=False):
        if not self.user_id or not self.password:
            raise ValueError("Set USERNAME and PASSWORD environment variables")
        
        if not force_refresh and self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at - timedelta(minutes=2):
                logger.info("Using cached token")
                return self.access_token
        
        logger.info("Requesting new token from Bhoonidhi API")
        response = requests.post(self.AUTH_URL, json={
            "userId": self.user_id,
            "password": self.password,
            "grant_type": "password"
        }, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        self.access_token = data["access_token"]
        self.token_expires_at = datetime.now() + timedelta(seconds=data.get("expires_in", 1200))
        self._save_token(self.access_token)
        logger.info("Successfully authenticated with Bhoonidhi")
        return self.access_token

    def scan(self, source, time_opts, space_opts, tmp_path, filter_online=False):
        """
        Search Bhoonidhi STAC catalog and return DataFrame.
        
        Args:
            source: Collection ID (e.g., 'ResourceSat-2A_LISS4-MX70_L2')
            filter_online: If True, only return items with 'Online' = 'Y'
        """
        token = self.authenticate()
        collection_id = source.rstrip('/').split('/')[-1] if source.startswith('http') else source
        
        search_params = {
            "collections": [collection_id],
            "limit": 500
        }
        
        if time_opts:
            start = time_opts.get('start')
            end = time_opts.get('end')
            if start and end:
                if isinstance(start, datetime):
                    start = start.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                elif not start.endswith('Z'):
                    start = f"{start}Z" if 'T' in start else f"{start}T00:00:00.000Z"
                
                if isinstance(end, datetime):
                    end = end.strftime('%Y-%m-%dT%H:%M:%S.999Z')
                elif not end.endswith('Z'):
                    end = f"{end}Z" if 'T' in end else f"{end}T23:59:59.999Z"
                
                search_params['datetime'] = f"{start}/{end}"
        
        if space_opts:
            if 'bbox' in space_opts:
                search_params['bbox'] = space_opts['bbox']
            elif 'geometry' in space_opts:
                search_params['intersects'] = space_opts['geometry']
        
        # Add filter for online products (as per API docs)
        if filter_online:
            search_params['filter'] = {
                "args": [{"property": "Online"}, "Y"],
                "op": "eq"
            }
            search_params['filter-lang'] = "cql2-json"
        
        headers = {"Authorization": f"Bearer {token}"}
        logger.info(f"Searching Bhoonidhi for collection: {collection_id}")
        
        response = requests.post(f"{self.STAC_URL}/search", json=search_params, headers=headers, timeout=60)
        
        if response.status_code == 401:
            token = self.authenticate(force_refresh=True)
            headers["Authorization"] = f"Bearer {token}"
            response = requests.post(f"{self.STAC_URL}/search", json=search_params, headers=headers, timeout=60)
        
        response.raise_for_status()
        features = response.json().get('features', [])
        logger.info(f"Found {len(features)} items")
        
        if not features:
            return pd.DataFrame(columns=['date', 'tile_name', 'collection', 'geometry', 'bbox', 'assets', 'properties', '_stac_item', 'engine_path', 'gdal_path'])
        
        rows = []
        for feature in features:
            props = feature.get('properties', {})
            item_id = feature.get('id')
            geometry = feature.get('geometry')
            bbox = feature.get('bbox')
            
            bands_info = None
            assets = feature.get('assets', {})
            if 'metadata' in assets:
                meta_href = assets['metadata'].get('href')
                if meta_href:
                    try:
                        meta_response = requests.get(meta_href, timeout=10)
                        if meta_response.status_code == 200:
                            meta_text = meta_response.text
                            # Parse band info from meta file
                            num_bands = None
                            band_numbers = None
                            for line in meta_text.split('\n'):
                                if line.startswith('NoOfBands='):
                                    num_bands = int(line.split('=')[1].strip())
                                elif line.startswith('BandNumbers='):
                                    band_numbers = line.split('=')[1].strip()
                            
                            if num_bands and band_numbers:
                                # Create band info list matching GDAL metadata structure
                                bands_info = []
                                for i, band_num in enumerate(band_numbers, start=1):
                                    bands_info.append({
                                        "source_idx": i,
                                        "description": f"Band_{band_num}",
                                        "dtype": "UInt16",  # Typical for ResourceSat
                                        "nodataval": None
                                    })
                                logger.debug(f"Extracted {num_bands} bands from metadata for {item_id}")
                    except Exception as e:
                        logger.warning(f"Failed to fetch metadata for {item_id}: {e}")
            
            rows.append({
                'date': props.get('datetime'),
                'tile_name': item_id,
                'collection': collection_id,
                'geometry': geometry,
                'bbox': bbox,
                'online_status': props.get('Online', 'N'),
                'properties': props,
                '_stac_item': feature,
                '_bands_info': bands_info,
                'engine_path': f"bhoonidhi://{collection_id}/{item_id}",
                'gdal_path': f"bhoonidhi-download://{collection_id}/{item_id}"
            })
        
        df = pd.DataFrame(rows)
        logger.info(f"Returning {len(df)} items with Online status")
        
        return df

    def _find_tif_files(self, directory):
        """Find TIF files in directory (excluding .aux.xml)."""
        return [os.path.join(root, f) for root, _, files in os.walk(directory)
                for f in files if f.lower().endswith('.tif') and not f.endswith('.aux.xml')]

    def sync(self, df, tmp_base_dir, overwrite=False):
        import zipfile
        
        token = self.authenticate()
        extract_dir = os.path.join(tmp_base_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        downloaded = []
        
        for band_tile in df.itertuples():
            tile = band_tile.tile
            item_id = tile.tile_name
            
            # Extract collection from engine_path
            collection = None
            if tile.engine_path and tile.engine_path.startswith('bhoonidhi://'):
                collection = tile.engine_path.replace('bhoonidhi://', '').split('/')[0]
            
            if not collection:
                logger.error(f"Cannot determine collection for {item_id}")
                continue
            
            if tile.gdal_path and os.path.exists(tile.gdal_path) and not overwrite:
                logger.info(f"Skipping {item_id}: already downloaded")
                continue
            
            item_extract_dir = os.path.join(extract_dir, item_id)
            if not overwrite and os.path.exists(item_extract_dir):
                tif_files = self._find_tif_files(item_extract_dir)
                if tif_files:
                    tile.gdal_path = tif_files[0]
                    logger.info(f"Using existing files for {item_id}")
                    continue
            
            download_url = f"{self.DOWNLOAD_URL}?id={item_id}&collection={collection}"
            output_file = os.path.join(tmp_base_dir, f"{item_id}.zip")
            
            logger.info(f"Downloading {item_id}...")
            try:
                headers = {"Authorization": f"Bearer {token}"}
                response = requests.get(download_url, headers=headers, stream=True, timeout=300)
                
                if response.status_code == 401:
                    token = self.authenticate(force_refresh=True)
                    headers["Authorization"] = f"Bearer {token}"
                    response = requests.get(download_url, headers=headers, stream=True, timeout=300)
                elif response.status_code == 404:
                    logger.error(f"Product not found: {item_id}")
                    continue
                elif response.status_code == 412:
                    logger.error("Too many concurrent downloads (max 3)")
                    raise Exception("Concurrent download limit exceeded")
                elif response.status_code == 504:
                    logger.error(f"Download timeout: {item_id}")
                    continue
                
                response.raise_for_status()
                
                with open(output_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Extracting {output_file}...")
                os.makedirs(item_extract_dir, exist_ok=True)
                with zipfile.ZipFile(output_file, 'r') as zip_ref:
                    zip_ref.extractall(item_extract_dir)
                
                tif_files = self._find_tif_files(item_extract_dir)
                if tif_files:
                    tile.gdal_path = tif_files[0]
                    try:
                        from osgeo import gdal, osr
                        ds = gdal.Open(tile.gdal_path, gdal.GA_ReadOnly)
                        if ds:
                            tile.geo_transform = ds.GetGeoTransform()
                            tile.projection = ds.GetProjection()
                            tile.x_size = ds.RasterXSize
                            tile.y_size = ds.RasterYSize
                            
                            srs = osr.SpatialReference()
                            srs.ImportFromWkt(ds.GetProjection())
                            crs_code = srs.GetAuthorityCode(None) or "4326"
                            tile.crs = f"EPSG:{crs_code}" if not crs_code.startswith("EPSG:") else crs_code
                            tile.length_unit = srs.GetLinearUnitsName() or "degree"
                            
                            # Get bands info matching GDAL metadata structure
                            tile.bands = [{
                                "source_idx": i,
                                "description": ds.GetRasterBand(i).GetDescription() or f"Band_{i}",
                                "dtype": gdal.GetDataTypeName(ds.GetRasterBand(i).DataType),
                                "nodataval": ds.GetRasterBand(i).GetNoDataValue()
                            } for i in range(1, ds.RasterCount + 1)]
                            
                            ds = None
                            logger.debug(f"Updated metadata for {item_id}")
                    except Exception as e:
                        logger.warning(f"Failed to update metadata: {e}")
                    downloaded.append(item_id)
                else:
                    logger.warning(f"No TIF files found in {item_extract_dir}")
            except Exception as e:
                logger.error(f"Download failed for {item_id}: {e}")
        
        logger.info(f"Downloaded {len(downloaded)} files")
        
        if downloaded:
            logger.info("Updating catalog with new metadata...")
            try:
                import json
                catalog_path = f"{tmp_base_dir}/catalog.csv"
                if os.path.exists(catalog_path):
                    seen = set()
                    catalog_data = []
                    for bt in df.itertuples():
                        if bt.tile.tile_name not in seen:
                            seen.add(bt.tile.tile_name)
                            catalog_data.append({
                                'date': bt.tile.date,
                                'tile_name': bt.tile.tile_name,
                                'gdal_path': bt.tile.gdal_path,
                                'engine_path': bt.tile.engine_path,
                                'geo_transform': str(bt.tile.geo_transform),
                                'projection': bt.tile.projection,
                                'x_size': bt.tile.x_size,
                                'y_size': bt.tile.y_size,
                                'crs': bt.tile.crs,
                                'length_unit': bt.tile.length_unit,
                                'bands': json.dumps(bt.tile.bands) if bt.tile.bands else None
                            })
                    pd.DataFrame(catalog_data).to_csv(catalog_path, header=True, index=False)
                    logger.info(f"Updated catalog at {catalog_path}")
            except Exception as e:
                logger.warning(f"Failed to update catalog: {e}")
        
        return df

