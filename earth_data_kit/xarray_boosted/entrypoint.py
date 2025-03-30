from xarray.backends import BackendEntrypoint, BackendArray
import os
import xarray as xr
from osgeo import gdal
import pandas as pd
import numpy as np
import traceback
import logging
import earth_data_kit.stitching.decorators as decorators
import affine
import concurrent.futures
import earth_data_kit.utilities.helpers as helpers

gdal.UseExceptions()

logger = logging.getLogger(__name__)


class EDKDatasetBackendArray(BackendArray):
    def __init__(
        self, filename_or_obj, shape, dtype, x_size, y_size, x_block_size, y_block_size
    ):
        self.filename_or_obj = filename_or_obj
        self.shape = shape
        self.dtype = dtype
        self.x_size = x_size
        self.y_size = y_size
        self.x_block_size = x_block_size
        self.y_block_size = y_block_size

    def __getitem__(self, key):
        return xr.core.indexing.explicit_indexing_adapter(
            key,
            self.shape,
            xr.core.indexing.IndexingSupport.BASIC,
            self._raw_indexing_method,
        )

    def _get_time_coords(self, key):
        if isinstance(key, slice):
            time_coords = []
            for t in range(*key.indices(self.shape[0])):
                time_coords.append(t)
        else:
            time_coords = [key]

        return time_coords

    def _get_band_nums(self, key):
        if isinstance(key, slice):
            band_nums = []
            for b in range(*key.indices(self.shape[1])):
                band_nums.append(int(b + 1))
        else:
            band_nums = [int(key + 1)]

        return band_nums

    def _get_x_y_coords(self, x_key, y_key):
        # TODO: Add code to pad x_key and y_key if they don't align with raster boundaries
        if not isinstance(x_key, slice):
            x_coords = slice(x_key, x_key + 1)
        else:
            x_coords = x_key

        if not isinstance(y_key, slice):
            y_coords = slice(y_key, y_key + 1)
        else:
            y_coords = y_key

        return x_coords, y_coords

    def _mask_nodata(self, data, nodataval):
        # np.isnan fails if nodataval is None because None cannot be converted to a numpy array
        # It also fails for other non-numeric types
        if nodataval is None or (
            isinstance(nodataval, (int, float)) and np.isnan(nodataval)
        ):
            return data

        data[data == nodataval] = np.nan
        return data

    def _scale_and_offset(self, data, scale, offset):
        if scale is None or (isinstance(scale, (int, float)) and np.isnan(scale)):
            scale = 1.0
        if offset is None or (isinstance(offset, (int, float)) and np.isnan(offset)):
            offset = 0.0
        return (data * scale) + offset

    @decorators.log_time
    @decorators.log_init
    def _read_band(self, fp, band_num, offsets, buf_sizes):
        ds = gdal.Open(fp)

        x_size = int(
            self.x_size - offsets[0]
            if (offsets[0] + buf_sizes[0]) > self.x_size
            else buf_sizes[0]
        )
        y_size = int(
            self.y_size - offsets[1]
            if (offsets[1] + buf_sizes[1]) > self.y_size
            else buf_sizes[1]
        )

        # Data returned will either be 2D or 3D, depending on whether we are selecting a single band or multiple bands
        data = ds.ReadAsArray(
            xoff=offsets[0],
            yoff=offsets[1],
            xsize=x_size,
            ysize=y_size,
            band_list=[band_num],
            buf_type=get_gdal_dtype(self.dtype),
        )
        band = ds.GetRasterBand(band_num)
        nodataval = band.GetNoDataValue()
        scale = band.GetScale()
        offset = band.GetOffset()

        ds = None

        data = self._mask_nodata(data, nodataval)
        data = self._scale_and_offset(data, scale, offset)

        return data

    def _read_bands(self, df, time_coords, band_nums, x_coords, y_coords):
        data = None
        time_arrays = []
        for time_coord in time_coords:
            band_arrays = []
            for band_num in band_nums:
                data = self._read_band(
                    df.iloc[time_coord].source,
                    band_num,
                    (x_coords.start, y_coords.start),
                    (x_coords.stop - x_coords.start, y_coords.stop - y_coords.start),
                )
                band_arrays.append(data)
            time_arrays.append(band_arrays)

        data = np.array(time_arrays)

        return data

    @decorators.log_time
    @decorators.log_init
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
        _df = pd.read_json(self.filename_or_obj)
        df = pd.DataFrame(_df["EDKDataset"]["VRTDatasets"])
        
        time_coords = self._get_time_coords(key[0])

        band_nums = self._get_band_nums(key[1])

        x_coords, y_coords = self._get_x_y_coords(key[2], key[3])

        data = self._read_bands(df, time_coords, band_nums, x_coords, y_coords)

        data = np.squeeze(data)

        if len(band_nums) == 1:
            data = np.transpose(data, axes=(1, 0))
        else:
            data = np.transpose(data, axes=(0, 2, 1))

        if isinstance(key[1], slice) and len(band_nums) == 1:
            data = np.array([data])

        if isinstance(key[0], slice):
            # Adding time dimension to the data, just need to wrap it in a list
            data = np.array([data])

        return data


def get_numpy_dtype(gdal_dtype):
    # Hardcoding dtype to float32 for now as we need to handle nodata values and they are only possible for float types
    return np.float32
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
        gdal.GDT_CFloat64: np.complex128,
    }
    return gdal_to_numpy_dtype.get(
        gdal_dtype, np.float32
    )  # Default to float32 if type not found


def get_spatial_coords(geotransform, width, height):
    # Extract geotransform parameters
    (
        upper_left_x,
        pixel_width,
        row_rotation,
        upper_left_y,
        column_rotation,
        pixel_height,
    ) = geotransform

    # Create affine transform
    transform = affine.Affine(
        pixel_width,
        row_rotation,
        upper_left_x,
        column_rotation,
        pixel_height,
        upper_left_y,
    )

    # Apply pixel center offset (0.5, 0.5)
    transform = transform * affine.Affine.translation(0.5, 0.5)

    # Picked from rioxarray
    if transform.is_rectilinear and (transform.b == 0 and transform.d == 0):
        x_coords, _ = transform * (np.arange(width), np.zeros(width))
        _, y_coords = transform * (np.zeros(height), np.arange(height))
    else:
        x_coords, y_coords = transform * np.meshgrid(
            np.arange(width),
            np.arange(height),
        )
    return {"x": x_coords, "y": y_coords}


def get_gdal_dtype(numpy_dtype):
    # Map numpy data types to GDAL data types
    numpy_to_gdal_dtype = {
        np.uint8: gdal.GDT_Byte,
        np.uint16: gdal.GDT_UInt16,
        np.int16: gdal.GDT_Int16,
        np.uint32: gdal.GDT_UInt32,
        np.int32: gdal.GDT_Int32,
        np.float32: gdal.GDT_Float32,
        np.float64: gdal.GDT_Float64,
        np.complex64: gdal.GDT_CFloat32,
        np.complex128: gdal.GDT_CFloat64,
    }
    # Convert numpy dtype objects to their type
    if hasattr(numpy_dtype, "type"):
        numpy_dtype = numpy_dtype.type

    return numpy_to_gdal_dtype.get(
        numpy_dtype, gdal.GDT_Float32
    )  # Default to Float32 if type not found


def open_edk_dataset(filename_or_obj):
    """Open an EDK dataset directly as an xarray Dataset without using DataArray."""
    try:
        # Read metadata from JSON
        _df = pd.read_json("/Users/siddhantgupta3/Desktop/Work/project-planet-bench/repos/private/edk/edk-tmp/tmp/gm-lc-v3/gm-lc-v3.json")
        df = pd.DataFrame(_df["EDKDataset"]["VRTDatasets"])

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

        spatial_coords = get_spatial_coords(src_ds.GetGeoTransform(), x_size, y_size)

        # Create coordinates
        coords = {
            "time": pd.DatetimeIndex(df.time),
            "band": np.arange(1, num_bands + 1, dtype=np.int32),
            "x": spatial_coords["x"],
            "y": spatial_coords["y"],
        }
        dims = ("time", "band", "x", "y")

        # TODO: Make the block size dynamic
        BLOCK_SIZE = 512
        da = xr.DataArray(
            data=xr.core.indexing.LazilyIndexedArray(
                EDKDatasetBackendArray(
                    filename_or_obj,
                    shape=(time_size, num_bands, x_size, y_size),
                    dtype=dtype,
                    x_size=x_size,
                    y_size=y_size,
                    x_block_size=BLOCK_SIZE,
                    y_block_size=BLOCK_SIZE,
                )
            ),
            name=filename_or_obj.split("/")[-1].split(".")[0],
            dims=dims,
            coords=coords,
            attrs={
                "source": filename_or_obj,
                "crs": src_ds.GetProjection() if src_ds.GetProjection() else None,
            },
        )

        # Close the GDAL dataset
        src_ds = None
        return da.to_dataset(promote_attrs=True)

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
