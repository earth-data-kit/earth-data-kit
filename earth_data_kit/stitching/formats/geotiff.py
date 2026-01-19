import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
import logging

logger = logging.getLogger(__name__)


class GeoTiffAdapter:
    def __init__(self) -> None:
        self.name = "GeoTiff"

    def create_tiles(self, scan_df, band_locator):
        is_bhoonidhi = scan_df["gdal_path"].apply(
            lambda x: isinstance(x, str) and x.startswith("bhoonidhi-download://")
        ).any()
        
        if is_bhoonidhi:
            wgs84_wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
            
            scan_df["projection"] = wgs84_wkt
            scan_df["crs"] = "EPSG:4326"
            scan_df["length_unit"] = "degree"
            scan_df["bands"] = scan_df.get("_bands_info", None)
            scan_df["x_size"] = 100
            scan_df["y_size"] = 100
            
            def calc_geo_transform(bbox):
                if bbox:
                    minx, miny, maxx, maxy = bbox
                    return (minx, (maxx - minx) / 100, 0, maxy, 0, -(maxy - miny) / 100)
                return None
            
            scan_df["geo_transform"] = scan_df["bbox"].apply(calc_geo_transform)
        else:
            metadata = commons.get_tiles_metadata(scan_df["gdal_path"].tolist(), band_locator)
            
            for idx, meta in enumerate(metadata):
                if meta:
                    scan_df.at[idx, "geo_transform"] = meta["geo_transform"]
                    scan_df.at[idx, "projection"] = meta["projection"]
                    scan_df.at[idx, "x_size"] = meta["x_size"]
                    scan_df.at[idx, "y_size"] = meta["y_size"]
                    scan_df.at[idx, "crs"] = meta["crs"]
                    scan_df.at[idx, "length_unit"] = meta["length_unit"]
                    scan_df.at[idx, "bands"] = meta["bands"]
            
            scan_df = scan_df[scan_df["geo_transform"].notna()].reset_index(drop=True)

        return Tile.from_df(scan_df)
