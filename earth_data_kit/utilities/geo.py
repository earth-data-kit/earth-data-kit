from osgeo import gdal, osr
import logging
from earth_data_kit.stitching import decorators
import earth_data_kit as edk
import shapely
import json
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_not_exception_type
from urllib.parse import urlparse, unquote
import os
from pystac_client import Client



logger = logging.getLogger(__name__)


@decorators.log_time
@decorators.log_init
def set_band_descriptions(filepath, bands):
    """
    Set the descriptions for bands in a GDAL raster file.

    Parameters:
        filepath (str): The path to the raster file to be updated.
        bands (list of str): A list of description strings for each raster band.

    Returns:
        None

    Raises:
        Exception: If the file cannot be opened or updated.
    """
    ds = gdal.Open(filepath, gdal.GA_Update)
    for idx in range(len(bands)):
        rb = ds.GetRasterBand(idx + 1)
        rb.SetDescription(bands[idx])
    del ds


def warp_and_get_extent(df_row):
    ds = gdal.Warp(
        "/vsimem/reprojected.tif", gdal.Open(df_row.gdal_path), dstSRS="EPSG:4326"
    )
    geo_transform = ds.GetGeoTransform()
    x_min = geo_transform[0]
    y_max = geo_transform[3]
    x_max = x_min + geo_transform[1] * ds.RasterXSize
    y_min = y_max + geo_transform[5] * ds.RasterYSize
    polygon = shapely.geometry.box(x_min, y_min, x_max, y_max, ccw=True)
    ds = None
    return polygon


def get_bbox_from_raster(raster_path):
    gdal_ds = gdal.Open(raster_path)
    gt = gdal_ds.GetGeoTransform()
    width = gdal_ds.RasterXSize
    height = gdal_ds.RasterYSize

    # Get the coordinates of the corners
    ulx = gt[0]
    uly = gt[3]
    lrx = gt[0] + width * gt[1] + height * gt[2]
    lry = gt[3] + width * gt[4] + height * gt[5]
    xmin, ymax, xmax, ymin = ulx, uly, lrx, lry

    # TODO: Make the fetching of CRS dynamic instead of hardcoding it to 3857

    lon_min, lat_min, lon_max, lat_max = transform_bbox(
        xmin, ymin, xmax, ymax, 3857, 4326
    )

    return lon_min, lat_min, lon_max, lat_max


def _get_bands(ds, band_locator="description"):
    bands = []
    band_count = ds.RasterCount

    for i in range(1, band_count + 1):
        band = ds.GetRasterBand(i)

        if band_locator == "description":
            band_name = band.GetDescription() 

        elif band_locator == "color_interp":
            band_name = gdal.GetColorInterpretationName(band.GetColorInterpretation()) 

        elif band_locator == "filename":
            # Derive band name from file path
            url = ds.GetDescription()
            if url.startswith("/vsicurl/"):
                url = url[len("/vsicurl/"):]
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            filename = os.path.splitext(filename)[0]
            filename = unquote(filename)
            band_name = filename

        else:
            raise ValueError(
                f"Invalid band locator: {band_locator}. "
                f"Should be one of: `description`, `color_interp`, `filename` "
            )

        b = {
            "source_idx": i,
            "description": (band_name if band_name != "" else "NoDescription"),
            "dtype": gdal.GetDataTypeName(band.DataType),
            "nodataval": band.GetNoDataValue(),
        }
        bands.append(b)

    return bands



class NonRetryableException(Exception):
    def __init__(self, message):
        super().__init__(message)


def get_metadata(raster_path, band_locator):
    # Figure out aws options
    ds = gdal.Open(raster_path)
    if ds is None:
        # Check if it's a 404 error by examining the error message
        error_msg = gdal.GetLastErrorMsg()
        if "404" in error_msg or "not found" in error_msg.lower():
            raise NonRetryableException("not found")
        raise Exception("Failed to open raster")

    gt = ds.GetGeoTransform()
    projection = ds.GetProjection()
    length_unit = ds.GetSpatialRef().GetAttrValue("UNIT")
    o = {
        "geo_transform": gt,
        "x_size": ds.RasterXSize,
        "y_size": ds.RasterYSize,
        "projection": projection,
        "crs": "EPSG:" + osr.SpatialReference(projection).GetAttrValue("AUTHORITY", 1),
        "bands": _get_bands(ds, band_locator),
        "length_unit": length_unit,
    }
    ds = None
    return o


def get_subdatasets(gdal_path):
    def get_subdatasets_recursive(path):
        result = []
        ds = gdal.Info(path, format="json")

        # Get subdatasets from the metadata json
        subdatasets = ds.get("metadata", {}).get("SUBDATASETS", {})

        # Extract subdataset paths from the json
        for key in subdatasets:
            if key.startswith("SUBDATASET_") and key.endswith("_NAME"):
                subdataset_path = subdatasets[key]
                result.append(subdataset_path)

                # Recursively check if this subdataset has its own subdatasets
                nested_subdatasets = get_subdatasets_recursive(subdataset_path)
                result.extend(nested_subdatasets)

        return result

    # Start the recursive process with the initial gdal_path
    return get_subdatasets_recursive(gdal_path)

from pystac_client import Client

def get_band_names_with_pystac(collection_id, catalog_url=None):
    if catalog_url is None:
        raise ValueError("catalog_url must be provided by the caller")

    catalog = Client.open(catalog_url)
    collection = catalog.get_collection(collection_id)

    band_names = []
    assets = collection.extra_fields.get("item_assets", {})

    for asset_name, meta in assets.items():
        roles = meta.get("roles", [])
        if any(role.lower() == "data" for role in roles):
            # If "bands" field exists, expand it
            bands_meta = meta.get("bands", [])
            if bands_meta:
                for b in bands_meta:
                    if "name" in b:
                        band_names.append(b["name"])
            else:
                # Fall back to using the asset name if no bands listed
                band_names.append(asset_name)

    return band_names
