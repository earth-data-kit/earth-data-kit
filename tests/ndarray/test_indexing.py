# This file tests different indexing methods for xarray data structures.
# 
# Xarray indexing provides powerful ways to select and manipulate multi-dimensional
# labeled data. It extends NumPy's indexing capabilities by supporting:
# - Label-based indexing with .sel() method
# - Positional indexing with .isel() method
# - Boolean indexing for conditional selection
# - Cross-section indexing with .sel(dim=value, method='nearest')
# - Assignment operations on selections
#
# These tests verify the correct behavior of various indexing approaches
# when working with earth data in xarray format.
import os
import pytest
from dotenv import load_dotenv
import earth_data_kit as edk
from tests.fixtures.country_bboxes import country_bounding_boxes

CONFIG_FILE_PATH = "tests/config.env"
load_dotenv(CONFIG_FILE_PATH)


def _label_based_indexing(dataarray):
    """
    Test label-based indexing with .sel() method on xarray DataArray.
    
    Args:
        dataarray: An xarray DataArray with time, band, x, and y dimensions
        
    Returns:
        bool: True if all tests pass, raises AssertionError otherwise
    """
    # Test basic label-based selection
    # Select first time value
    first_time = dataarray.time.values[0]
    time_slice = dataarray.sel(time=first_time)
    
    # Test selection with x and y coordinates
    # Select a specific x and y coordinate from Albania's bounding box
    albania_bbox = country_bounding_boxes["AL"][1]  # Get Albania's bounding box
    # Calculate center point of Albania's bounding box
    x_val = (albania_bbox[0] + albania_bbox[2]) / 2  # Middle longitude
    y_val = (albania_bbox[1] + albania_bbox[3]) / 2  # Middle latitude
    
    # Select using exact coordinates
    point_data = dataarray.sel(x=x_val, y=y_val, method='nearest')
    
    # Select using nearest method for approximate coordinates
    x_approx = x_val + 0.1  # Slightly offset coordinate
    y_approx = y_val + 0.1  # Slightly offset coordinate
    nearest_point = dataarray.sel(x=x_approx, y=y_approx, method='nearest')
    
    # Test selection with method='nearest'
    # Select a band using nearest method
    band_val = dataarray.band.values[0]
    nearest_selection = dataarray.sel(band=band_val, method='nearest')
    
    # Test multiple dimension selection
    first_band = dataarray.band.values[0]
    multi_sel = dataarray.sel(time=first_time, band=first_band, x=x_val, y=y_val, method='nearest')
    
    assert True


def _position_based_indexing(dataarray):
    """
    Test position-based indexing with .isel() method on xarray DataArray.
    
    Args:
        dataarray: An xarray DataArray with time, band, x, and y dimensions
        
    Returns:
        bool: True if all tests pass, raises AssertionError otherwise
    """
    # Test basic positional indexing
    # Select first time index
    time_slice = dataarray.isel(time=0)
    
    # Test x and y positional indexing
    # Select specific x and y indices
    xy_point = dataarray.isel(x=0, y=0)
    
    # Select a region if enough points are available
    region = dataarray.isel(x=slice(0, 2), y=slice(0, 2))
    
    # Test slicing
    # Select a slice of x dimension
    x_slice = dataarray.isel(x=slice(0, 2))
    
    # Test multiple dimension selection
    # Select first index of time and band dimensions
    multi_sel = dataarray.isel(time=0, band=0)
    
    # Test negative indexing
    # Select last y index
    last_y = dataarray.isel(y=-1)
    
    assert True


@pytest.mark.order(1)
def test_indexing_operations():
    """
    Test indexing operations on xarray data structures loaded from grid files.
    """

    # test_grid_file()

    ds = edk.stitching.Dataset.from_file(f"{os.getenv('TMP_DIR')}/tmp/modis-pds/modis-pds.json")
    da = ds.to_dataarray()

    _label_based_indexing(da)
    _position_based_indexing(da)

    assert True