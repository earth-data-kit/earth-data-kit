import os
import cv2
import numpy as np
import io
from flask import jsonify


def get_os_params():
    file_path = os.getenv("FILE_PATH", None)
    band_idx = int(os.getenv("BAND_IDX", None))

    return file_path, band_idx


def get_transparent_tile():
    # Create a transparent image of 256x256 pixels
    transparent_img = np.zeros((256, 256, 4), dtype=np.uint8)
    # Set alpha channel to 0 (fully transparent)
    transparent_img[:, :, 3] = 0

    # Encode the transparent image to PNG format
    success, encoded_img = cv2.imencode(".png", transparent_img)
    if not success:
        return jsonify({"error": "Failed to encode transparent image"}), 500

    # Create an in-memory file-like object to store the PNG
    content = io.BytesIO(encoded_img.tobytes()).getvalue()

    return content
