from flask import Response, jsonify
import earth_data_kit as edk
import logging
from rio_tiler.io import XarrayReader, Reader
from rio_tiler.profiles import img_profiles
import rio_tiler
from rio_tiler.colormap import cmap
from earth_data_kit.viz_server.commons import get_os_params, get_transparent_tile

# Convert numpy array to image
from flask import Response

logger = logging.getLogger(__name__)


def get_image_bounds(filepath):
    bounds = edk.utilities.geo.get_bbox_from_raster(filepath)

    return jsonify({"bbox": bounds}), 200


def get_tile(z, x, y):
    viridis = cmap.get("viridis")
    filepath, band_idx = get_os_params()

    with rio_tiler.io.Reader(filepath) as src:
        try:
            img = src.tile(x, y, z, indexes=[band_idx])
            content = img.render(
                img_format="PNG", **img_profiles.get("png"), colormap=viridis
            )
            return Response(content, mimetype="image/png")
        except rio_tiler.errors.TileOutsideBounds as e:
            logger.debug(f"Tile outside bounds: {e}")
            return Response(get_transparent_tile(), mimetype="image/png")
        except Exception as e:
            logger.error(f"Error: {e}")
            return jsonify({"error": str(e)}), 500
