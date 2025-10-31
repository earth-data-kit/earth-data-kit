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
from fixtures.country_bboxes import country_bounding_boxes

os.environ["AWS_REGION"] = "ap-south-1"
os.environ["AWS_NO_SIGN_REQUEST"] = "NO"


@pytest.mark.order(1)
def test_label_based_indexing():
    dataarray = edk.stitching.Dataset.dataarray_from_file(
        f"/app/data/tmp/global-land-cover/global-land-cover.json"
    )

    # Test basic label-based selection
    # Select first time value
    first_time = dataarray.time.values[0]
    time_slice = dataarray.sel(time=first_time)
    assert not time_slice.isnull().all().any()

    # Test selection with x and y coordinates
    # Select a specific x and y coordinate from Albania's bounding box
    albania_bbox = country_bounding_boxes["AL"][1]  # Get Albania's bounding box
    # Calculate center point of Albania's bounding box
    x_val = (albania_bbox[0] + albania_bbox[2]) / 2  # Middle longitude
    y_val = (albania_bbox[1] + albania_bbox[3]) / 2  # Middle latitude

    # Select using exact coordinates
    point_data = dataarray.sel(x=x_val, y=y_val, method="nearest")
    assert not point_data.isnull().all().any()

    # Select using nearest method for approximate coordinates
    x_approx = x_val + 0.1  # Slightly offset coordinate
    y_approx = y_val + 0.1  # Slightly offset coordinate
    nearest_point = dataarray.sel(x=x_approx, y=y_approx, method="nearest")
    assert not nearest_point.isnull().all().any()

    # Test selection with method='nearest'
    # Select a band using nearest method
    band_val = dataarray.band.values[0]
    nearest_selection = dataarray.sel(band=band_val, method="nearest")
    assert not nearest_selection.isnull().all().any()

    # Test multiple dimension selection
    first_band = dataarray.band.values[0]
    multi_sel = dataarray.sel(
        time=first_time, band=first_band, x=x_val, y=y_val, method="nearest"
    )
    assert not multi_sel.isnull().all().any()

    assert True


@pytest.mark.order(1)
def position_based_indexing():
    """
    Test position-based indexing with .isel() method on xarray DataArray.

    Args:
        dataarray: An xarray DataArray with time, band, x, and y dimensions

    Returns:
        bool: True if all tests pass, raises AssertionError otherwise
    """
    dataarray = edk.stitching.Dataset.dataarray_from_file(
        f"/app/data/tmp/global-land-cover/global-land-cover.json"
    )
    # Test basic positional indexing
    # Select first time index
    time_slice = dataarray.isel(time=0)
    assert not time_slice.isnull().all().any()

    # Test x and y positional indexing
    # Select specific x and y indices
    xy_point = dataarray.isel(x=0, y=0)
    assert not xy_point.isnull().all().any()

    # Select a region if enough points are available
    region = dataarray.isel(x=slice(0, 2), y=slice(0, 2))
    assert not region.isnull().all().any()

    # Test slicing
    # Select a slice of x dimension
    x_slice = dataarray.isel(x=slice(0, 2))
    assert not x_slice.isnull().all().any()

    # Test multiple dimension selection
    # Select first index of time and band dimensions
    multi_sel = dataarray.isel(time=0, band=0)
    assert not multi_sel.isnull().all().any()

    # Test negative indexing
    # Select last y index
    last_y = dataarray.isel(y=-1)
    assert not last_y.isnull().all().any()

    assert True
