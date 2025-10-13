import pytest
import earth_data_kit as edk
import xarray as xr


def test_edk_import():
    """Test that earth_data_kit can be imported successfully"""
    assert edk is not None


def test_edk_version():
    """Test that earth_data_kit has a valid version string"""
    assert edk.__version__ is not None


def test_s5cmd_version():
    """Test that s5cmd version is available"""
    assert edk.get_s5cmd_version() is not None


def test_gdal_version():
    """Test that gdal version is available"""
    assert edk.get_gdal_version() is not None


def test_xarray_edk_engine():
    """Test that xarray recognizes edk_dataset as an available engine"""

    engines = xr.backends.list_engines() # type: ignore
    assert "edk_dataset" in engines
