import folium
import matplotlib as mpl
from osgeo import gdal
import numpy as np
import pandas as pd
import earth_data_kit as edk


class Folium:
    def __init__(self, da):
        self.da = da

    def _create_cmap(self, base_color):
        cmap = mpl.colors.LinearSegmentedColormap.from_list(
            "custom_red", ["white", base_color]
        )

        def colormap(x):
            return cmap(x, 1)

        return colormap

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
            image=edk.utilities.helpers.scale_to_255(arr).T,
            bounds=[[lat_min, lon_min], [lat_max, lon_max]],
            interactive=True,
            colormap=self._create_cmap("red"),
        )

        band.add_to(m)
        folium.LayerControl().add_to(m)
        return m
