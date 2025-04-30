from flask import Flask, jsonify, request, send_from_directory
import time
import datetime
from earth_data_kit.viz_server.tile import get_image_bounds, get_tile
from earth_data_kit.viz_server.commons import get_os_params
import logging
from flask_cors import CORS
import os

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Record the start time when the server starts
start_time = time.time()


@app.route("/", methods=["GET"])
@app.route("/status", methods=["GET"])
def status():
    """Status endpoint that returns uptime and alive status."""
    current_time = time.time()
    uptime_seconds = current_time - start_time

    # Format uptime as a readable string
    uptime = str(datetime.timedelta(seconds=int(uptime_seconds)))

    return jsonify({"alive": True, "uptime": uptime})


@app.route("/image/bounds", methods=["GET"])
def get_bounds():
    try:
        filepath, _ = get_os_params()
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    return get_image_bounds(filepath)


@app.route("/image/tile/<int:z>/<int:x>/<int:y>.png", methods=["GET"])
def tile(z, x, y):
    return get_tile(z, x, y)


# Enable CORS for all routes
CORS(app, expose_headers="*")


def serve(file_path, band_idx=1):
    os.environ["FILE_PATH"] = file_path
    os.environ["BAND_IDX"] = str(band_idx)

    app.run(host="0.0.0.0", port=5432)
