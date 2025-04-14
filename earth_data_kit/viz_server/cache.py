import logging
import time
from typing import Dict, Any, Tuple
import earth_data_kit as edk
import xarray as xr

logger = logging.getLogger(__name__)

# Global in-memory dataarray cache
_cached_dataarray = {}


def init_dataset_cache(filepath, time_value, band_value):
    """
    Initialize the dataset cache by loading the dataset into memory.

    Args:
        filepath (str): Path to the dataset file
        time_value (str): Timestamp to select
        band_value (int): Band index to select

    Returns:
        bool: True if initialization was successful, False otherwise

    Raises:
        ValueError: If time_value or band_value is not provided
    """
    try:
        global _cached_dataarray
        if time_value is None or band_value is None:
            raise ValueError("Both time_value and band_value must be provided")

        ds = edk.stitching.Dataset.from_file(filepath)
        da = ds.to_dataarray()

        # Select the specific time and band and store in the nested cache
        selected_da = da.sel(time=time_value, band=int(band_value)).edk.read_as_array()
        _cached_dataarray = xr.DataArray(
            selected_da,
            dims=["x", "y"],
            coords={"x": da.x, "y": da.y},
            attrs={"crs": da.crs},
        ).transpose("y", "x")

        return True
    except Exception as e:
        logger.error(e)
        return False


def get_cached_array():
    """
    Retrieve a cached numpy array for the specified dataset, time, and band.

    Args:
        filepath (str): Path to the dataset file
        time_value (str): Timestamp to select
        band_value (int): Band index to select

    Returns:
        numpy.ndarray: The cached numpy array if found, None otherwise
    """
    global _cached_dataarray
    return _cached_dataarray
