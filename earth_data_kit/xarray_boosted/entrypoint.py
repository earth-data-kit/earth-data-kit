from xarray.backends import BackendEntrypoint, BackendArray
import os
import glob
import xarray as xr
from osgeo import gdal
import pandas as pd
import numpy as np
import dask
from typing import Any, Iterable, Tuple, Union, List, Dict, Optional
import traceback
import rioxarray as rio
from xarray.backends import BackendEntrypoint, BackendArray
import os
import glob
import xarray as xr
from osgeo import gdal
import pandas as pd
import numpy as np
import dask
import traceback

class EDKDatasetBackendArray(BackendArray):
    def __init__(self, filename_or_obj, shape, dtype):
        self.filename_or_obj = filename_or_obj
        self.shape = shape
        self.dtype = dtype
        self.lock = xr.backends.locks.SerializableLock()

    def __getitem__(self, key):
        return xr.core.indexing.explicit_indexing_adapter(
            key, self.shape, xr.core.indexing.IndexingSupport.BASIC, 
            self._raw_indexing_method
        )
    
    def _raw_indexing_method(self, key):
        """Handle basic indexing (integers and slices only).
        
        Parameters
        ----------
        key : tuple
            A tuple of integers and/or slices.
            
        Returns
        -------
        numpy.ndarray
            The indexed data.
        """
        print ("key", key)
        # Calculate output shape
        output_shape = ()
        
        for idx, k in enumerate(key):
            if isinstance(k, slice):
                output_shape += (len(range(*k.indices(self.shape[idx]))),)
            else:
                pass
        
        if output_shape == ():
            return np.random.randint(0, 100, (1, 1, 1, 1),  dtype=self.dtype)
        else:
            return np.random.randint(0, 100, output_shape, dtype=self.dtype)


def get_numpy_dtype(gdal_dtype):
    # Map GDAL data types to numpy data types
    gdal_to_numpy_dtype = {
        gdal.GDT_Byte: np.uint8,
        gdal.GDT_UInt16: np.uint16,
        gdal.GDT_Int16: np.int16,
        gdal.GDT_UInt32: np.uint32,
        gdal.GDT_Int32: np.int32,
        gdal.GDT_Float32: np.float32,
        gdal.GDT_Float64: np.float64,
        gdal.GDT_CInt16: np.complex64,
        gdal.GDT_CInt32: np.complex64,
        gdal.GDT_CFloat32: np.complex64,
        gdal.GDT_CFloat64: np.complex128
    }
    return gdal_to_numpy_dtype.get(gdal_dtype, np.float32)  # Default to float32 if type not found

def open_edk_dataset(filename_or_obj):
    """Open an EDK dataset directly as an xarray Dataset without using DataArray."""
    try:
        # Read metadata from XML
        df = pd.read_xml(filename_or_obj)
        
        if len(df) == 0:
            raise ValueError("No raster data found in the input file")
            
        # Get the source path from the first row
        source_path = df.iloc[0].source
        
        # Open the dataset with GDAL
        src_ds = gdal.Open(source_path)
        if src_ds is None:
            raise ValueError(f"Could not open raster file: {source_path}")
        
        # Get dimensions
        x_size = src_ds.RasterXSize
        y_size = src_ds.RasterYSize
        num_bands = src_ds.RasterCount
        time_size = df.shape[0]

        # Get data type from GDAL
        band = src_ds.GetRasterBand(1)
        gdal_dtype = band.DataType
        
        
        # Get corresponding numpy dtype
        dtype = get_numpy_dtype(gdal_dtype)  # Default to float32 if type not found
        
        # Create coordinates
        coords = {
            "time": pd.DatetimeIndex(df.time),
            "band": np.arange(1, num_bands + 1, dtype=np.int32),
            "x": np.arange(x_size, dtype=np.int32),
            "y": np.arange(y_size, dtype=np.int32),
        }

        da = xr.DataArray(
            data=xr.core.indexing.LazilyIndexedArray(
                EDKDatasetBackendArray(
                    filename_or_obj, 
                    shape=(time_size, num_bands, x_size, y_size), 
                    dtype=dtype
                )
            ),
            name=filename_or_obj.split("/")[-1].split(".")[0],
            dims=("time", "band", "x", "y"),
            coords=coords,
            attrs={
                "source": filename_or_obj,
                "crs": src_ds.GetProjection() if src_ds.GetProjection() else None,
            }
        )
        
        # Close the GDAL dataset
        src_ds = None
        return da.to_dataset()
        
    except Exception as e:
        print(f"Error opening dataset: {e}")
        traceback.print_exc()
        raise

class EDKDatasetBackend(BackendEntrypoint):
    def open_dataset(
        self,
        filename_or_obj,
        *,
        drop_variables=None,
        # other backend specific keyword arguments
        # `chunks` and `cache` DO NOT go here, they are handled by xarray
    ):
        return open_edk_dataset(filename_or_obj)

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj):
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".my_format", ".my_fmt"}

    description = "Use .my_format files in Xarray"

    url = "https://link_to/your_backend/documentation"
