import logging
from earth_data_kit.utilities.geo import get_subdatasets
from osgeo import gdal
from earth_data_kit.stitching.classes.tile import Tile
import earth_data_kit.utilities as utilities
import concurrent.futures
from tqdm import tqdm

logger = logging.getLogger(__name__)


class NetCDFAdapter:
    def __init__(self) -> None:
        self.name = "NetCDF"

    @staticmethod
    def get_bands_from_netcdf(nc_dataset, key):
        bands = []
        idx = 0
        # Assume nc_dataset is a netCDF4.Dataset or xarray.Dataset
        # and key is a variable name or similar
        try:
            variables = list(nc_dataset.variables.keys())
        except Exception:
            variables = []
        for var_name in variables:
            var = nc_dataset.variables[var_name]
            o = {
                "nodataval": getattr(var, "_FillValue", None),
                "dtype": str(var.dtype) if hasattr(var, "dtype") else None,
                "source_idx": idx + 1,
                "description": var_name,
            }
            idx += 1
            bands.append(o)
        return bands

    @staticmethod
    def get_projection_info(nc_dataset):
        # Try to extract projection info from netCDF attributes
        # This is a best-effort implementation
        crs = None
        geo_transform = None
        try:
            if hasattr(nc_dataset, "crs"):
                crs = nc_dataset.crs
            elif hasattr(nc_dataset, "attrs") and "crs" in nc_dataset.attrs:
                crs = nc_dataset.attrs["crs"]
            # Try to get geotransform from attributes
            if hasattr(nc_dataset, "geotransform"):
                geo_transform = nc_dataset.geotransform
            elif hasattr(nc_dataset, "attrs") and "geotransform" in nc_dataset.attrs:
                geo_transform = nc_dataset.attrs["geotransform"]
        except Exception as e:
            logger.warning(f"Could not extract projection info: {e}")
        return {"crs": crs, "geo_transform": geo_transform}

    
    def create_tiles(self, df, band_locator="description"):
        # df is a DataFrame with a "gdal_path" column pointing to NetCDF files
        # This function will extract metadata and bands for each NetCDF file and including its subdatasets
        for df_row in df.itertuples():
            subdataset_paths = get_subdatasets(df_row.gdal_path)
            for subdataset_path in subdataset_paths:
                metadata = utilities.geo.get_metadata(subdataset_path, band_locator)
                logger.info(metadata)


    def get_subdatasets(ds):
        paths = []
        for subds in ds.GetSubDatasets():
            paths.append(subds[0])
        return paths