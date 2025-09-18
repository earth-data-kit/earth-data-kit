import logging
from osgeo import gdal
from earth_data_kit.stitching.classes.tile import Tile
import earth_data_kit.utilities as utilities
import concurrent.futures
from tqdm import tqdm
import pandas as pd
from pyproj import CRS
from earth_data_kit.stitching.classes.tile import Tile

logger = logging.getLogger(__name__)


class NetCDFAdapter:
    def __init__(self) -> None:
        self.name = "NetCDF"
    
    def create_tiles(self, df, band_locator="description"):
        # df is a DataFrame with a "gdal_path" column pointing to NetCDF files
        # This function will extract metadata and bands for each NetCDF file and including its subdatasets
        rows = []

        with concurrent.futures.ProcessPoolExecutor(max_workers=utilities.helpers.get_processpool_workers()) as executor:
            futures = []
            for df_row in df.itertuples():
                futures.append(executor.submit(process_row, tuple(df_row), band_locator))

            for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Getting metadata"):
                try:
                    row_list = f.result()
                    rows.extend(row_list)
                except Exception as e:
                    logger.error(f"Error processing future: {e}", exc_info=True)

        df = pd.DataFrame(rows, columns=["date", "tile_name", "engine_path", "gdal_path", "geo_transform", "projection", "x_size", "y_size", "crs", "length_unit", "bands"])

        tiles = Tile.from_df(df)
        return tiles

def process_row(df_row_tuple, band_locator):
    # Accept a tuple instead of a pandas namedtuple to avoid pickling issues
    # Unpack the tuple manually
    _, date, gdal_path, engine_path, tile_name = df_row_tuple
    ds = gdal.Open(gdal_path)
    rows = []
    for subdataset in ds.GetSubDatasets():
        row = [date, tile_name, engine_path, subdataset[0]]
        # Remove the break so all subdatasets are processed
        metadata = utilities.geo.get_metadata(subdataset[0], band_locator)
        row.append(metadata.get("geo_transform"))
        row.append(metadata.get("projection"))
        row.append(metadata.get("x_size"))
        row.append(metadata.get("y_size"))
        row.append(metadata.get("crs"))
        row.append(metadata.get("length_unit"))
        # Passing array of jsons in a dataframe "bands" column
        # Replace the "description" field in each band dict with varname. A single subdataset has single band in netCDF
        varname = subdataset[0].split(":")[2]
        bands = metadata.get("bands")
        if bands is not None:
            bands[0]["description"] = varname
        row.append(bands)

        rows.append(row)
    return rows