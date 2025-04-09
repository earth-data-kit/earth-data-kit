import xarray as xr
import logging
import pandas as pd
from osgeo import gdal
import earth_data_kit as edk
import folium
from earth_data_kit.xarray_boosted.classes.folium import Folium
from earth_data_kit.xarray_boosted.classes.deck import DeckGL
import numpy as np
import earth_data_kit.stitching.decorators as decorators

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
