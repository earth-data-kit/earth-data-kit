from flask import Response, jsonify
import time
import earth_data_kit as edk
import logging

# Convert numpy array to image
from flask import Response, send_file
import numpy as np
from PIL import Image
import io
import os
import cv2

logger = logging.getLogger(__name__)


def get_image_data(filepath, band_value, time_value):
    """
    Get image data for the specified file using xarray directly.

    Args:
        filepath (str): Path to the raster dataset (COG, VRT, etc.)
        band_value (int): Band index to read.
        time_value (str): Timestamp to select data from.

    Returns:
        Response: Flask response with the PNG image data
    """
    ds = edk.stitching.Dataset.from_file(filepath)
    da = ds.to_dataarray()

    da = da.sel(time=time_value, band=int(band_value))
    # Normalize data to 0-255 range for image visualization
    scaled_data = edk.utilities.helpers.scale_to_255(da.values.T)

    # Create a masked array where NaN values are masked for transparency handling
    masked_data = np.ma.masked_array(scaled_data, mask=np.isnan(scaled_data))

    # Convert to uint8 for image creation (only non-masked values)
    data_uint8 = masked_data.filled(0).astype("uint8")

    # Create a grayscale image using OpenCV
    grayscale_img = data_uint8.copy()

    # Convert to RGBA (OpenCV uses BGR format)
    # Create a 4-channel image (BGRA)
    rgba_img = cv2.cvtColor(grayscale_img, cv2.COLOR_GRAY2BGRA)

    # Make NaN values transparent by setting alpha channel to 0
    # Get the alpha channel (4th channel)
    alpha_mask = np.where(np.isnan(scaled_data), 0, 255).astype(np.uint8)
    rgba_img[:, :, 3] = alpha_mask

    # Encode the image to PNG format
    success, encoded_img = cv2.imencode(".png", rgba_img)
    if not success:
        return jsonify({"error": "Failed to encode image"}), 500

    # Create an in-memory file-like object to store the PNG
    rawBytes = io.BytesIO(encoded_img.tobytes())

    return send_file(rawBytes, mimetype="image/png")


def get_image_bounds(filepath, band, time):
    ds = edk.stitching.Dataset.from_file(filepath)
    da = ds.to_dataarray()
    da = da.sel(time=time, band=int(band))
    bounds = da.attrs["bbox"]
    transformed_bounds = None

    # Transform bounds from EPSG:3857 to EPSG:4326
    if bounds and len(bounds) == 4:
        # Use edk.utilities.transform_bbox function to transform from EPSG:3857 to EPSG:4326
        transformed_bounds = edk.utilities.transform.transform_bbox(
            *bounds, source_epsg=3857, target_epsg=4326
        )
    return jsonify({"bbox": transformed_bounds}), 200
