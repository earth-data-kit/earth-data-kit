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

    def _create_cmap(self, vmin, vmax):
        viridis = cm.LinearColormap(cm.linear.viridis.colors, vmin=vmin, vmax=vmax)

        def get_color(x):
            if np.isnan(x):
                return (1, 1, 1, 0)
            return viridis.rgba_floats_tuple(x)

        return get_color

    def plot(self):
        # Reading the index/catalog file
        _df = pd.read_json(self.da.attrs["source"])
        df = pd.DataFrame(_df["EDKDataset"]["VRTDatasets"])

        # Getting first vrt to figure out raster bounds
        vrt_fp = df.loc[0, "source"]

        lon_min, lat_min, lon_max, lat_max = edk.utilities.geo.get_bbox_from_raster(
            vrt_fp
        )

        m = folium.Map(
            location=[(lat_max + lat_min) / 2, (lon_max + lon_min) / 2], zoom_start=9
        )

        arr = self.da.values
        band = folium.raster_layers.ImageOverlay(
            image=arr.T,
            bounds=[[lat_min, lon_min], [lat_max, lon_max]],
            interactive=True,
            colormap=self._create_cmap(vmin=np.nanmin(arr), vmax=np.nanmax(arr)),
        )

        band.add_to(m)
        folium.LayerControl().add_to(m)
        return m
