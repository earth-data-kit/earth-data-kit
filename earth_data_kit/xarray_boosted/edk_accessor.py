import xarray as xr
from tqdm import tqdm
import concurrent.futures
import logging
import pandas as pd
from osgeo import gdal
import earth_data_kit as edk
import folium
from earth_data_kit.xarray_boosted.classes.folium import Folium
from earth_data_kit.xarray_boosted.classes.deck import DeckGL
import numpy as np
import earth_data_kit.stitching.decorators as decorators
import earth_data_kit.utilities.helpers as helpers

logger = logging.getLogger(__name__)


@xr.register_dataarray_accessor("edk")
class EDKAccessor:
    def __init__(self, xarray_obj, *args, **kwargs):
        self.da = xarray_obj

    @decorators.log_time
    @decorators.log_init
    def plot(self, engine="folium", *args, **kwargs):
        # Handle time and band selection with defaults if not specified
        if "time" in self.da.dims:
            logger.error(
                "Time dimension found in the DataArray. We currently do not support plotting time series data."
            )
            return
        if "band" in self.da.dims:
            logger.error(
                "Band dimension found in the DataArray. We currently do not support plotting multi-band data."
            )
            return

        if engine == "folium":
            return Folium(self.da).plot()
        elif engine == "deck":
            return DeckGL(self.da).plot()
        else:
            raise ValueError(f"Invalid engine: {engine}")

    def __read_chunk__(self, x_start, y_start, chunk_size=512):
        x_end = min(x_start + chunk_size, self.da.shape[0])
        y_end = min(y_start + chunk_size, self.da.shape[1])

        # print (f"reading {y_start} {x_start} {y_end} {x_end}")
        return (y_start, x_start, self.da.isel(y=slice(y_start, y_end), x=slice(x_start, x_end)).values)
    
    @decorators.log_time
    @decorators.log_init
    def read_as_array(self):
        # Handle time and band selection with defaults if not specified
        if "time" in self.da.dims:
            logger.error(
                "Time dimension found in the DataArray. We currently do not support plotting time series data."
            )
            return
        if "band" in self.da.dims:
            logger.error(
                "Band dimension found in the DataArray. We currently do not support plotting multi-band data."
            )
            return

        x_size, y_size = self.da.shape
        result = np.full((x_size, y_size), np.nan, dtype=self.da.dtype)
        chunk_size = 512
        # Create chunks
        chunks = [(x, y) for x in range(0, x_size, chunk_size) 
                        for y in range(0, y_size, chunk_size)]
        
        # Process chunks in parallel using concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=helpers.get_threadpool_workers()) as executor:
            futures = [executor.submit(self.__read_chunk__, x, y, chunk_size) 
                    for x, y in chunks]
            
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Reading data in chunks", unit="chunk"):
                y_start, x_start, chunk = future.result()
                chunk_x_size, chunk_y_size = chunk.shape
                result[x_start:x_start+chunk_x_size, y_start:y_start+chunk_y_size] = chunk
        
        return result