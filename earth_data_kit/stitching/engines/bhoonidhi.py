import logging
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

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
        """Load token from cache file if valid (< 18 minutes old)."""
        try:
            if os.path.exists(self.TOKEN_FILE):
                mtime = datetime.fromtimestamp(os.path.getmtime(self.TOKEN_FILE))
                if datetime.now() - mtime < timedelta(minutes=18):
                    with open(self.TOKEN_FILE, 'r') as f:
                        self.access_token = f.read().strip()
                    self.token_expires_at = mtime + timedelta(minutes=20)
                    logger.info("Loaded cached Bhoonidhi token")
        except Exception as e:
            logger.debug(f"Could not load cached token: {e}")

    def _save_token(self, token):
        """Save token to cache file."""
        try:
            with open(self.TOKEN_FILE, 'w') as f:
                f.write(token)
        except Exception as e:
            logger.warning(f"Could not save token to cache: {e}")

    def authenticate(self, force_refresh=False):
        """Authenticate and get JWT token (cached for 18 minutes)."""
        if not self.user_id or not self.password:
            raise ValueError("Set USERNAME and PASSWORD environment variables")
        
        # Reuse valid token
        if not force_refresh and self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at - timedelta(minutes=2):
                logger.info("Using cached token")
                return self.access_token
        
        # Request new token
        logger.info("Requesting new token from Bhoonidhi API")
        payload = {
            "userId": self.user_id,
            "password": self.password,
            "grant_type": "password"
        }
        
        response = requests.post(self.AUTH_URL, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        self.access_token = data["access_token"]
        self.token_expires_at = datetime.now() + timedelta(seconds=data.get("expires_in", 1200))
        self._save_token(self.access_token)
        
        logger.info("Successfully authenticated with Bhoonidhi")
        return self.access_token

    def scan(self, source, time_opts, space_opts, tmp_path, filter_online=False):
        """
        Search Bhoonidhi catalog and return DataFrame with STAC items.
        
        Args:
            filter_online: If True, only return items with 'Online' = 'Y'
        """
        token = self.authenticate()
        
        # Extract collection ID from URL if needed
        collection_id = source.rstrip('/').split('/')[-1] if source.startswith('http') else source
        
        # Prepare search parameters
        search_params = {
            "collections": [collection_id],
            "limit": 1000
        }
        
        # Add time filter
        if time_opts:
            start = time_opts.get('start')
            end = time_opts.get('end')
            if start and end:
                # Convert to ISO format with Z suffix
                if isinstance(start, datetime):
                    start = start.strftime('%Y-%m-%dT%H:%M:%SZ')
                if isinstance(end, datetime):
                    end = end.strftime('%Y-%m-%dT%H:%M:%SZ')
                search_params['datetime'] = f"{start}/{end}"
        
        # Add spatial filter
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
        
        # Make authenticated request
        headers = {"Authorization": f"Bearer {token}"}
        logger.info(f"Searching Bhoonidhi for collection: {collection_id}")
        
        response = requests.post(f"{self.STAC_URL}/search", json=search_params, headers=headers, timeout=60)
        
        if response.status_code == 404:
            logger.warning(f"No results found for collection {collection_id}")
            features = []
        else:
            response.raise_for_status()
            features = response.json().get('features', [])
        
        logger.info(f"Found {len(features)} items")
        
        if not features:
            return pd.DataFrame(columns=['date', 'tile_name', 'collection', 'geometry', 'bbox', 'assets', 'properties', '_stac_item', 'engine_path', 'gdal_path'])
        
        # Parse features into DataFrame
        # Note: Bhoonidhi items only have metadata/thumbnail assets, not data files
        rows = []
        for feature in features:
            props = feature.get('properties', {})
            item_id = feature.get('id')
            
            rows.append({
                'date': props.get('datetime'),
                'tile_name': item_id,
                'collection': collection_id,
                'geometry': feature.get('geometry'),
                'bbox': feature.get('bbox'),
                'assets': feature.get('assets', {}),
                'properties': props,
                '_stac_item': feature,
                'engine_path': f"bhoonidhi://{collection_id}/{item_id}",  # Placeholder URI
                'gdal_path': f"bhoonidhi://{collection_id}/{item_id}"  # Placeholder URI, will be updated during sync()
            })
        
        df = pd.DataFrame(rows)
        logger.info(f"Returning {len(df)} items")
        
        return df

    def sync(self, df, tmp_base_dir, overwrite=True):
        """
        Download data products from Bhoonidhi and update DataFrame with VSI paths.
        
        Only downloads items with 'Online' = 'Y' status.
        Max 3 concurrent downloads per user.
        Returns DataFrame with updated gdal_path using GDAL VSI /vsizip/ prefix.
        """
        token = self.authenticate()
        os.makedirs(tmp_base_dir, exist_ok=True)
        
        downloaded = []
        for idx, row in df.iterrows():
            item_id = row['tile_name']
            
            # Extract collection from row
            collection = row.get('collection') if 'collection' in row and pd.notna(row.get('collection')) else None
            if not collection:
                for key in ['engine_path', 'gdal_path']:
                    path = row.get(key, '')
                    if path and path.startswith('bhoonidhi://'):
                        collection = path.replace('bhoonidhi://', '').split('/')[0]
                        break
            
            if not collection:
                logger.error(f"Cannot determine collection for item {item_id}")
                continue
            
            # Check if online
            props = row.get('properties', {})
            if isinstance(props, dict) and props.get('Online') != 'Y' and props.get('Online') is not None:
                logger.warning(f"Skipping {item_id}: Not online")
                continue
            
            # Build download URL
            download_url = f"{self.DOWNLOAD_URL}?id={item_id}&collection={collection}"
            
            # Output file path
            output_file = os.path.join(tmp_base_dir, f"{item_id}.zip")
            
            if not overwrite and os.path.exists(output_file):
                logger.info(f"Skipping {item_id}: Already exists")
                downloaded.append(output_file)
                continue
            
            # Download
            logger.info(f"Downloading {item_id}...")
            try:
                headers = {"Authorization": f"Bearer {token}"}
                response = requests.get(download_url, headers=headers, stream=True, timeout=300)
                
                if response.status_code == 404:
                    logger.error(f"Product not found: {item_id}")
                    continue
                elif response.status_code == 412:
                    logger.error(f"Concurrent download limit exceeded. Wait and retry.")
                    raise Exception("Too many concurrent downloads")
                elif response.status_code == 504:
                    logger.error(f"Download interrupted: {item_id}")
                    continue
                
                response.raise_for_status()
                
                # Save file
                with open(output_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Downloaded: {output_file}")
                downloaded.append(output_file)
                
            except Exception as e:
                logger.error(f"Download failed for {item_id}: {e}")
        
        logger.info(f"Downloaded {len(downloaded)} files")
        
        # Update DataFrame with VSI paths for downloaded files
        for idx, row in df.iterrows():
            zip_file = os.path.join(tmp_base_dir, f"{row['tile_name']}.zip")
            if os.path.exists(zip_file):
                df.at[idx, 'gdal_path'] = f"/vsizip/{zip_file}"
        
        return df
