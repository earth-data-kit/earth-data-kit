import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
import logging

logger = logging.getLogger(__name__)


class GeoTiffAdapter:
    def __init__(self) -> None:
        self.name = "GeoTiff"

    def create_tiles(self, scan_df, band_locator):
        metadata = commons.get_tiles_metadata(
            scan_df["gdal_path"].tolist(), band_locator
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
        scan_df = scan_df[scan_df["geo_transform"].notna()].reset_index(drop=True)

        tiles = Tile.from_df(scan_df)
        return tiles
