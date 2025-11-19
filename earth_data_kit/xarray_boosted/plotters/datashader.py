import numpy as np
import xarray as xr
import holoviews as hv
import geoviews as gv
import panel as pn
import datashader as ds
import cartopy.crs as ccrs
from holoviews.operation.datashader import rasterize, shade

# Optional GDAL import for coordinate transforms
try:
    from osgeo import osr
    GDAL_AVAILABLE = True
except ImportError:
    GDAL_AVAILABLE = False

# Initialize extensions
hv.extension("bokeh")
gv.extension("bokeh")
pn.extension()

def mercator_to_latlon(x, y):
    """Convert Web Mercator (EPSG:3857) to geographic (lat, lon)."""
    lon = x / 20037508.342789244 * 180.0
    lat = np.arctan(np.exp(y / 20037508.342789244 * np.pi)) * 360.0 / np.pi - 90.0
    return lon, lat

class Datashader:
    def __init__(self, da):
        """
        Supports:
          - 2D DataArray (lon, lat) -> Single band, grayscale
          - 3D DataArray with 1 band -> Single band, grayscale
          - 3D DataArray with 3 bands -> RGB composite
          - 3D DataArray with 4 bands -> RGBA composite (alpha channel)
        """
        if "lon" not in da.coords or "lat" not in da.coords:
            raise ValueError("DataArray must contain 'lon' and 'lat' coordinates.")

        self.da = da
        if da.ndim == 3:
            self.band_names = [str(b.item()) if hasattr(b, "item") else str(b) 
                             for b in da.coords.get("band", range(da.shape[0])).values]
            self.num_bands = len(self.band_names)
        else:
            self.band_names = ["Band 1"]
            self.num_bands = 1

    def _normalize_band(self, band_data):
        """Normalize band data to 0-1 range for RGB display."""
        vmin, vmax = np.nanpercentile(band_data, [2, 98])
        normalized = np.clip((band_data - vmin) / (vmax - vmin), 0, 1)
        return normalized

    def _prepare_rgb_image(self):
        """Prepare RGB or RGBA composite using HoloViews RGB element."""
        da = self.da
        
        if self.num_bands < 3:
            raise ValueError("RGB plotting requires at least 3 bands")
        
        lon, lat = da["lon"].values, da["lat"].values
        x_merc, y_merc = ds.utils.lnglat_to_meters(lon, lat)
        
        # Get RGB bands (first 3) and normalize to 0-1 range
        r_data = self._normalize_band(da.isel(band=0).values)
        g_data = self._normalize_band(da.isel(band=1).values)
        b_data = self._normalize_band(da.isel(band=2).values)
        
        # Stack RGB data (height, width, 3)
        rgb_data = np.stack([r_data, g_data, b_data], axis=-1)
        
        # Add alpha channel if we have 4 bands
        if self.num_bands >= 4:
            a_data = self._normalize_band(da.isel(band=3).values)
            rgb_data = np.concatenate([rgb_data, a_data[..., np.newaxis]], axis=-1)
        
        # Create RGB element with bounds
        bounds = (x_merc.min(), y_merc.min(), x_merc.max(), y_merc.max())
        rgb = hv.RGB(rgb_data, bounds=bounds, kdims=['x', 'y'])
        
        return rgb

    def _prepare_single_band_image(self, band_idx=0, cmap="gray"):
        """Rasterize single band with colormap."""
        da = self.da

        if da.ndim == 3:
            img = da.isel(band=band_idx)
        else:
            img = da

        lon, lat = img["lon"].values, img["lat"].values
        vals = img.values

        x_merc, y_merc = ds.utils.lnglat_to_meters(lon, lat)
        quad = hv.QuadMesh((x_merc, y_merc, vals), kdims=["x", "y"])
        return shade(rasterize(quad), cmap=cmap)

    def basemap(self):
        return gv.tile_sources.OSM()

    def plot(self):
        da = self.da
        
        # Enable RGB mode for 3+ bands
        use_rgb_mode = self.num_bands >= 3
        
        # Setup widgets based on mode
        if use_rgb_mode:
            # RGB/RGBA mode - no band selector or colormap needed
            mode_label = pn.pane.Markdown(
                f"**Display Mode:** {'RGBA' if self.num_bands >= 4 else 'RGB'} Composite\n\n"
                f"**Bands:** {', '.join(self.band_names[:4 if self.num_bands >= 4 else 3])}"
            )
            band_selector = None
            cmap_selector = None
        elif self.num_bands > 1:
            # Multiple bands - allow band selection
            band_selector = pn.widgets.Select(
                name="Select Band", 
                options=self.band_names, 
                value=self.band_names[0]
            )
            mode_label = pn.pane.Markdown(
                f"**{self.num_bands} bands available** - Select individual bands to view"
            )
        else:
            # Single band
            band_selector = None
            mode_label = None

        # Opacity control
        alpha_slider = pn.widgets.FloatSlider(
            name="Opacity", 
            start=0.0, 
            end=1.0, 
            step=0.01, 
            value=0.9
        )

        # Info card
        popup_card = pn.Card(
            pn.pane.Markdown("### Click on the map\nPixel info will appear here"),
            title="Data Info",
            collapsible=False,
            width=300,
            height=250,
        )

        lon_values = da["lon"].values
        lat_values = da["lat"].values
        lon_dim, lat_dim = da["lon"].dims[0], da["lat"].dims[0]

        # Tap callback
        def _on_tap(x, y):
            if x is None or y is None:
                popup_card.objects = [pn.pane.Markdown("### Click on the map")]
                return

            try:
                # Coordinate conversion
                if GDAL_AVAILABLE:
                    srs_3857 = osr.SpatialReference(); srs_3857.ImportFromEPSG(3857)
                    srs_4326 = osr.SpatialReference(); srs_4326.ImportFromEPSG(4326)
                    trans = osr.CoordinateTransformation(srs_3857, srs_4326)
                    lat, lon, _ = trans.TransformPoint(x, y)
                else:
                    lon, lat = mercator_to_latlon(x, y)

                # Find nearest indices
                if lon_values.ndim == 2:
                    distances = np.sqrt((lon_values - lon) ** 2 + (lat_values - lat) ** 2)
                    lat_idx, lon_idx = np.unravel_index(distances.argmin(), distances.shape)
                else:
                    lon_idx = np.abs(lon_values - lon).argmin()
                    lat_idx = np.abs(lat_values - lat).argmin()

                lon_idx = int(np.clip(lon_idx, 0, da.sizes[lon_dim] - 1))
                lat_idx = int(np.clip(lat_idx, 0, da.sizes[lat_dim] - 1))

                # Get pixel values
                if da.ndim == 3:
                    # Show all band values
                    pixel_values = []
                    for i, band_name in enumerate(self.band_names):
                        selected_data = da.isel(y=lat_idx, x=lon_idx, band=i)
                        raw_val = selected_data.values
                        if hasattr(raw_val, 'item'):
                            val = float(raw_val.item())
                        elif np.isscalar(raw_val):
                            val = float(raw_val)
                        else:
                            val = float('nan')
                        val_display = "NaN" if np.isnan(val) else f"{val:.4f}"
                        pixel_values.append(f"**{band_name}:** {val_display}")
                    
                    values_text = "\n".join(pixel_values)
                else:
                    selected_data = da.isel(y=lat_idx, x=lon_idx)
                    raw_val = selected_data.values
                    if hasattr(raw_val, 'item'):
                        val = float(raw_val.item())
                    elif np.isscalar(raw_val):
                        val = float(raw_val)
                    else:
                        val = float('nan')
                    val_display = "NaN" if np.isnan(val) else f"{val:.6f}"
                    values_text = f"**Value:** {val_display}"

                popup_card.objects = [
                    pn.pane.Markdown(
                        f"### Location Info\n"
                        f"**Longitude:** {lon:.5f}°\n"
                        f"**Latitude:** {lat:.5f}°\n\n"
                        f"{values_text}"
                    )
                ]
            except Exception as e:
                popup_card.objects = [pn.pane.Markdown(f"### Error\n{str(e)[:100]}")]

        # Map setup
        proj = ccrs.GOOGLE_MERCATOR
        
        # Create the appropriate visualization
        if use_rgb_mode:
            # RGB/RGBA mode - simple and clean
            tap_stream = hv.streams.Tap(x=None, y=None)
            tap_stream.add_subscriber(_on_tap)

            @pn.depends(alpha_slider.param.value)
            def update_plot(alpha_val):
                rgb_img = self._prepare_rgb_image()
                rgb_with_alpha = rgb_img.opts(alpha=alpha_val)
                tap_stream.source = rgb_with_alpha
                return (self.basemap() * rgb_with_alpha).opts(
                    hv.opts.Overlay(projection=proj, frame_width=700, frame_height=500)
                )

            return pn.Row(pn.Column(mode_label, alpha_slider, update_plot), popup_card)

        elif band_selector:
            # Multi-band mode with band selection
            tap_stream = hv.streams.Tap(x=None, y=None)
            tap_stream.add_subscriber(_on_tap)

            @pn.depends(band_selector.param.value, alpha_slider.param.value)
            def update_plot(selected_band, alpha_val):
                band_idx = self.band_names.index(selected_band)
                shaded = self._prepare_single_band_image(band_idx, cmap="viridis")
                shaded_with_alpha = shaded.opts(alpha=alpha_val)
                tap_stream.source = shaded_with_alpha
                return (self.basemap() * shaded_with_alpha).opts(
                    hv.opts.Overlay(projection=proj, frame_width=700, frame_height=500)
                )

            controls = [alpha_slider, band_selector]
            if mode_label:
                controls.insert(0, mode_label)
            
            return pn.Row(pn.Column(*controls, update_plot), popup_card)

        else:
            # Single band mode
            tap_stream = hv.streams.Tap(x=None, y=None)
            tap_stream.add_subscriber(_on_tap)

            @pn.depends(alpha_slider.param.value)
            def update_plot(alpha_val):
                shaded = self._prepare_single_band_image(0, cmap="viridis")
                shaded_with_alpha = shaded.opts(alpha=alpha_val)
                tap_stream.source = shaded_with_alpha
                return (self.basemap() * shaded_with_alpha).opts(
                    hv.opts.Overlay(projection=proj, frame_width=700, frame_height=500)
                )

            return pn.Row(pn.Column(alpha_slider, update_plot), popup_card)
