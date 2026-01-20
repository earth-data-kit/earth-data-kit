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

    def _download_and_extract(self, item_id, collection_id, tmp_path, headers):
        """Download and extract a single item."""
        item_extract_dir = os.path.join(tmp_path, "raw-data", item_id)
        
        if os.path.exists(item_extract_dir):
            return item_extract_dir
        
        download_url = f"{self.DOWNLOAD_URL}?id={item_id}&collection={collection_id}"
        output_file = os.path.join(tmp_path, f"{item_id}.zip")
        
        logger.info(f"Downloading {item_id}...")
        response = requests.get(download_url, headers=headers, stream=True, timeout=300)
        response.raise_for_status()
        
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        os.makedirs(item_extract_dir, exist_ok=True)
        with zipfile.ZipFile(output_file, 'r') as zip_ref:
            zip_ref.extractall(item_extract_dir)
        
        os.remove(output_file)
        return item_extract_dir

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator=None):
        """
        Search Bhoonidhi STAC catalog and return DataFrame.
        
        Args:
            source: Collection ID (e.g., 'ResourceSat-2A_LISS4-MX70_L2')
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
        
        # Always filter for online items only
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
            return pd.DataFrame(columns=['date', 'tile_name', 'engine_path', 'gdal_path'])
        
        rows = []
        for feature in features:
            props = feature.get('properties', {})
            item_id = feature.get('id')
            
            try:
                item_extract_dir = self._download_and_extract(item_id, collection_id, tmp_path, headers)
                tif_files = [os.path.join(root, f) for root, _, files in os.walk(item_extract_dir)
                            for f in files if f.lower().endswith('.tif') and not f.endswith('.aux.xml')]
                
                # Create a row for each TIF file (each band is a separate file)
                for tif_file in tif_files:
                    rows.append({
                        'date': props.get('datetime'),
                        'tile_name': item_id,
                        'engine_path': f"bhoonidhi://{collection_id}/{item_id}",
                        'gdal_path': tif_file
                    })
            except Exception as e:
                logger.error(f"Failed to process {item_id}: {e}")
                continue
        
        df = pd.DataFrame(rows)
        logger.info(f"Returning {len(df)} items with downloaded files")
        
        return df
