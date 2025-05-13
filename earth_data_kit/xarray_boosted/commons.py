from osgeo import gdal
import numpy as np


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
