import folium
import matplotlib as mpl
from osgeo import gdal
import numpy as np
import pandas as pd
import earth_data_kit as edk
import branca.colormap as cm
import logging

logger = logging.getLogger(__name__)


class Folium:
    def __init__(self, da):
        self.da = da

    def _get_crs(self):
        """
        Get the coordinate reference system (CRS) from the DataArray.

        Returns:
            int or None: The EPSG code of the CRS if available, otherwise None.
        """
        return int(self.da.coords["spatial_ref"].values)

    def _create_cmap(self, vmin, vmax):
        viridis = cm.LinearColormap(cm.linear.viridis.colors, vmin=vmin, vmax=vmax)

        def get_color(x):
            if np.isnan(x):
                return (1, 1, 1, 0)
            return viridis.rgba_floats_tuple(x)

        return get_color

    def plot(self):
        crs = self._get_crs()

        xmin, ymin, xmax, ymax = (
            self.da.coords["x"].values[0],
            self.da.coords["y"].values[0],
            self.da.coords["x"].values[-1],
            self.da.coords["y"].values[-1],
        )

        lng_min, lat_min, lng_max, lat_max = edk.utilities.transform.transform_bbox(
            xmin, ymin, xmax, ymax, crs, 4326
        )

        m = folium.Map(
            location=[(lat_max + lat_min) / 2, (lng_max + lng_min) / 2], zoom_start=9
        )

        arr = self.da.edk.read_as_array()
        band = folium.raster_layers.ImageOverlay(
            image=arr.T,
            bounds=[[lat_min, lng_min], [lat_max, lng_max]],
            interactive=True,
            colormap=self._create_cmap(vmin=np.nanmin(arr), vmax=np.nanmax(arr)),
        )

        band.add_to(m)
        folium.LayerControl().add_to(m)
        return m
