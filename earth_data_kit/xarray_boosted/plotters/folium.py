import folium
import matplotlib as mpl
from osgeo import gdal
import numpy as np
import pandas as pd
import earth_data_kit.utilities as utilities
import branca.colormap as cm
import logging

logger = logging.getLogger(__name__)


class Folium:
    def __init__(self, da):
        self.da = da

    def _create_cmap(self, vmin, vmax, colors=None):
        if colors is None:
            cmap = cm.LinearColormap(cm.linear.viridis.colors, vmin=vmin, vmax=vmax)
        else:
            cmap = cm.LinearColormap(colors, vmin=vmin, vmax=vmax)

        def get_color(x):
            if np.isnan(x):
                return (1, 1, 1, 0)
            return cmap.rgba_floats_tuple(x)

        return get_color

    def plot(self, colors=None, opacity=1):

        crs = self.da.edk._get_epsg_code()

        xmin, ymin, xmax, ymax = (
            self.da.coords["x"].values[0],
            self.da.coords["y"].values[0],
            self.da.coords["x"].values[-1],
            self.da.coords["y"].values[-1],
        )

        lng_min, lat_min, lng_max, lat_max = utilities.transform.transform_bbox(
            xmin, ymin, xmax, ymax, crs, 4326
        )

        m = folium.Map(
            location=[(lat_max + lat_min) / 2, (lng_max + lng_min) / 2], zoom_start=4
        )

        arr = self.da.edk.read_as_array()
        band = folium.raster_layers.ImageOverlay(
            image=arr.T,
            bounds=[[lat_min, lng_min], [lat_max, lng_max]],
            interactive=True,
            colormap=self._create_cmap(
                vmin=np.nanmin(arr), vmax=np.nanmax(arr), colors=colors
            ),
            opacity=opacity,
        )

        band.add_to(m)
        folium.LayerControl().add_to(m)
        return m
