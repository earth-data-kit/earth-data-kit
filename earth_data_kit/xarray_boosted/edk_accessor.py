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
    """
    The EDK Accessor extends xarray's functionality by providing additional methods for working with Earth Data Kit datasets.
    This accessor is automatically registered with xarray when edk is imported, allowing you to access these extended capabilities through the `.edk` namespace on xarray objects.
    """

    def __init__(self, xarray_obj, *args, **kwargs):
        self.da = xarray_obj

    @decorators.log_time
    @decorators.log_init
    def plot(self, engine="folium", **kwargs):
        """
        Plot the DataArray using the specified visualization engine.

        This function visualizes geospatial data contained in the DataArray.
        Currently supports two visualization engines: folium and deck.gl.

        Args:
            engine (str, optional): The visualization engine to use. Defaults to "folium". Possible values are "folium" and "deck".

        Returns:
            object: The map object from the selected visualization engine. For "folium" engine, returns a folium.Map object. For "deck" engine, returns None as the visualization is displayed in browser.

        Notes:
            - Currently does not support plotting time series data
            - Currently does not support plotting multi-band data
            - Requires the DataArray to have spatial dimensions (x, y)

        Raises:
            ValueError: If an invalid engine is specified
        """
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
            return Folium(self.da).plot(**kwargs)
        elif engine == "deck":
            return DeckGL(self.da).plot(**kwargs)
        else:
            raise ValueError(f"Invalid engine: {engine}")

    def __read_chunk__(self, x_start, y_start, chunk_size=512):
        x_end = min(x_start + chunk_size, self.da.shape[0])
        y_end = min(y_start + chunk_size, self.da.shape[1])

        # print (f"reading {y_start} {x_start} {y_end} {x_end}")
        return (
            y_start,
            x_start,
            self.da.isel(y=slice(y_start, y_end), x=slice(x_start, x_end)).values,
        )

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
        chunks = [
            (x, y)
            for x in range(0, x_size, chunk_size)
            for y in range(0, y_size, chunk_size)
        ]

        # Process chunks in parallel using concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = [
                executor.submit(self.__read_chunk__, x, y, chunk_size)
                for x, y in chunks
            ]

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Reading data in chunks",
                unit="chunk",
            ):
                y_start, x_start, chunk = future.result()
                chunk_x_size, chunk_y_size = chunk.shape
                result[
                    x_start : x_start + chunk_x_size, y_start : y_start + chunk_y_size
                ] = chunk

        return result
