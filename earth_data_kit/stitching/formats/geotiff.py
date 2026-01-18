import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
from osgeo import gdal
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


class GeoTiffAdapter:
    def __init__(self) -> None:
        self.name = "GeoTiff"

    def create_tiles(self, scan_df, band_locator):
        # Expand paths for zip archives if needed
        # When a zip contains multiple TIF files, we need to create multiple rows
        expanded_rows = []
        gdal_paths = []
        
        for idx, row in scan_df.iterrows():
            path = row["gdal_path"]
            
            # Skip placeholder URIs (e.g., bhoonidhi:// - files not downloaded yet)
            if path and path.startswith("bhoonidhi://"):
                gdal_paths.append(None)
                expanded_rows.append(row)
            elif path and path.startswith("/vsizip/") and not path.endswith(".tif"):
                # This is a zip archive, find all .tif files inside
                tif_files = self._expand_vsizip_path(path)
                for tif_path in tif_files:
                    gdal_paths.append(tif_path)
                    # Create a copy of the row for each TIF file
                    new_row = row.copy()
                    new_row["gdal_path"] = tif_path
                    expanded_rows.append(new_row)
            else:
                gdal_paths.append(path)
                expanded_rows.append(row)
        
        # Create new dataframe with expanded rows
        if expanded_rows:
            scan_df = pd.DataFrame(expanded_rows).reset_index(drop=True)
        
        metadata = commons.get_tiles_metadata(
            gdal_paths, band_locator
        )

        # Add new columns to the dataframe
        scan_df["geo_transform"] = None
        scan_df["projection"] = None
        scan_df["x_size"] = None
        scan_df["y_size"] = None
        scan_df["crs"] = None
        scan_df["length_unit"] = None
        scan_df["bands"] = None

        for idx in range(len(metadata)):
            if metadata[idx] is None:
                continue
            scan_df.at[idx, "geo_transform"] = metadata[idx]["geo_transform"]
            scan_df.at[idx, "projection"] = metadata[idx]["projection"]
            scan_df.at[idx, "x_size"] = metadata[idx]["x_size"]
            scan_df.at[idx, "y_size"] = metadata[idx]["y_size"]
            scan_df.at[idx, "crs"] = metadata[idx]["crs"]
            scan_df.at[idx, "length_unit"] = metadata[idx]["length_unit"]
            # Passing array of jsons in a dataframe "bands" column
            scan_df.at[idx, "bands"] = metadata[idx]["bands"]
        # Only filter out rows without metadata if they had valid paths
        # Keep rows with empty gdal_path (e.g., Bhoonidhi before sync) or placeholder URIs
        scan_df = scan_df[
            (scan_df["geo_transform"].notna()) | 
            (scan_df["gdal_path"] == "") |
            (scan_df["gdal_path"].str.startswith("bhoonidhi://", na=False))
        ].reset_index(drop=True)

        tiles = Tile.from_df(scan_df)
        return tiles
    
    def _expand_vsizip_path(self, vsizip_path):
        tif_files = []
        try:
            # List contents of the zip using GDAL's VSI
            file_list = gdal.ReadDir(vsizip_path)
            if file_list:
                for filename in file_list:
                    if filename.lower().endswith('.tif') or filename.lower().endswith('.tiff'):
                        full_path = f"{vsizip_path}/{filename}"
                        tif_files.append(full_path)
                    elif '.' not in filename:  # Might be a directory
                        # Check subdirectories
                        subdir_path = f"{vsizip_path}/{filename}"
                        subdir_list = gdal.ReadDir(subdir_path)
                        if subdir_list:
                            for subfile in subdir_list:
                                if subfile.lower().endswith('.tif') or subfile.lower().endswith('.tiff'):
                                    full_path = f"{subdir_path}/{subfile}"
                                    tif_files.append(full_path)
        except Exception as e:
            logger.error(f"Failed to read zip contents from {vsizip_path}: {e}")
        
        return tif_files
