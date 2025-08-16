# from __future__ import (division, print_function, absolute_import, unicode_literals)

# import argparse
# import os
import os.path
# import shutil
# import sys
# import time
import requests
from pathlib import Path
import logging
import pandas as pd  # For data handling and DataFrame operations
import geopandas as gpd  # For handling spatial data and KML files
import re            # For regular expression pattern matching
import copy         # For deep copying objects
from urllib.parse import urlparse  # For parsing URLs into components

logger = logging.getLogger(__name__)

class HTTPS:
    def __init__(self, auth_token=None) -> None:
        """Initialize HTTPS downloader
        Args:
            auth_token (str, optional): Bearer token for authentication
        """
        self.name = "HTTPS"  # Name identifier for the engine
        # Define standard HTTP headers
        self.headers = {
            'User-Agent': 'earth_data_kit/1.0 (Python Downloader)',  # Identifies our client
            'Accept': '*/*'  # Accepts any content type
        }
        # Add authorization header if token provided
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'
        # Create a session to reuse connections
        self.session = requests.Session()

    def get_patterns(self, source, time_opts, space_opts):
        """Generate all possible file patterns based on time and space options
        Args:
            source (str): URL pattern with placeholders
            time_opts (dict): Contains 'start' and 'end' dates
            space_opts (dict): Contains spatial options like 'grid_file' and 'matcher'
        """
        # Initialize empty DataFrame to store patterns
        patterns_df = pd.DataFrame()

        # Create date range from start to end dates
        start = time_opts["start"]
        end = time_opts["end"]
        # Generate all dates in range and store in DataFrame
        patterns_df["date"] = pd.date_range(start=start, end=end, inclusive="both")
        # Apply date formatting to source pattern
        patterns_df["search_path"] = patterns_df["date"].dt.strftime(source)

        # Get bounding box from space options
        bbox = space_opts["bbox"]
        
        # If no grid file specified, return time-based patterns only
        if ("grid_file" not in space_opts) or (
            ("grid_file" in space_opts) and (space_opts["grid_file"] is None)
        ):
            return patterns_df

        # Get grid file path and matcher function
        grid_file = space_opts["grid_file"]
        matcher = space_opts["matcher"]

        # Handle KML grid files
        if grid_file.endswith(".kml") or grid_file.endswith(".KML"):
            # Read KML file with optional bounding box filter
            grid_df = gpd.read_file(grid_file, driver="kml", bbox=bbox)
            
            # Store matched variables for each grid cell
            space_vars = []
            for grid in grid_df.itertuples():
                space_vars.append(matcher(grid))

            # Create new patterns combining time and space
            new_patterns = []
            for row in patterns_df.itertuples():
                # Find all variables in curly braces
                matches = re.findall(r"({.[^}]*})", row.search_path)
                # Replace variables with values from grid
                for var in space_vars:
                    tmp_p = copy.copy(row.search_path)
                    for m in matches:
                        # Remove curly braces and replace with grid value
                        tmp_p = tmp_p.replace(
                            m, var[m.replace("{", "").replace("}", "")]
                        )
                    new_patterns.append([row.date, tmp_p])
            
            # Create new DataFrame with combined patterns
            new_patterns_df = pd.DataFrame(
                new_patterns, columns=["date", "search_path"]
            )
            return new_patterns_df
        else:
            raise Exception("drivers other than kml are not supported")

    def download_file(self, url: str, dest_path: Path) -> bool:
        """Download a single file from URL to destination path
        Args:
            url (str): URL to download from
            dest_path (Path): Where to save the file
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Skip download if file exists and is not empty
            if dest_path.exists() and dest_path.stat().st_size > 0:
                logger.debug(f"Skipping existing file: {dest_path}")
                return True

            # Stream download to handle large files efficiently
            with self.session.get(url, headers=self.headers, stream=True) as response:
                response.raise_for_status()  # Raise error for bad status codes
                with open(dest_path, 'wb') as f:
                    # Download in chunks to manage memory
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return False

    def scan(self, source, time_opts, space_opts, tmp_base_dir):
        """Scan for available files matching pattern and optionally download
        Args:
            source (str): URL pattern template
            time_opts (dict): Time range options
            space_opts (dict): Spatial options
            tmp_base_dir (str): Directory for downloads
        Returns:
            DataFrame: Information about available files
        """
        # Generate all possible file patterns
        patterns_df = self.get_patterns(source, time_opts, space_opts)
        
        # Create list for valid URLs
        valid_urls = []
        
        # Check each URL pattern
        for _, row in patterns_df.iterrows():
            url = row['search_path']
            try:
                # Verify URL exists without downloading
                response = self.session.head(url, headers=self.headers)
                if response.ok:
                    valid_urls.append({
                        'date': row['date'],
                        'key': url,
                    })
            except requests.RequestException as e:
                logger.debug(f"URL not accessible: {url}")
                continue

        # Create DataFrame from valid URLs
        inv_df = pd.DataFrame(valid_urls)
        
        # Return empty DataFrame if no files found
        if len(inv_df) == 0:
            return pd.DataFrame(columns=["date", "engine_path", "gdal_path", "tile_name"])

        # Extract tile name from URL
        inv_df["tile_name"] = inv_df["key"].apply(
            lambda x: ".".join(urlparse(x).path.split("/")[-1].split(".")[:-1])
        )
        
        # Create GDAL virtual filesystem path
        inv_df["gdal_path"] = inv_df["key"].apply(
            lambda x: f"/vsicurl/{x}"
        )
        
        # Store original URL as engine path
        inv_df["engine_path"] = inv_df["key"]

        # Return DataFrame with required columns
        return inv_df[["date", "engine_path", "gdal_path", "tile_name"]]
         
