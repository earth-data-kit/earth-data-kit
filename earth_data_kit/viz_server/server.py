from flask import Flask, jsonify, request, send_from_directory
import time
import datetime
from earth_data_kit.viz_server.tile import get_tile, get_image_data, get_image_bounds
import logging
from flask_cors import CORS
import os
import requests
import threading

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Record the start time when the server starts
start_time = time.time()


@app.route("/status", methods=["GET"])
def status():
    """Status endpoint that returns uptime and alive status."""
    current_time = time.time()
    uptime_seconds = current_time - start_time

    # Format uptime as a readable string
    uptime = str(datetime.timedelta(seconds=int(uptime_seconds)))

    return jsonify({"alive": True, "uptime": uptime})


def validate_os_params():
    filepath = os.getenv("SOURCE", None)
    band = os.getenv("BAND", None)
    time = os.getenv("TIME", None)

    if not filepath:
        return jsonify({"error": "Missing filepath parameter"}), 400
    if not band:
        return jsonify({"error": "Missing band parameter"}), 400
    if not time:
        return jsonify({"error": "Missing time parameter"}), 400

    return filepath, band, time


@app.route("/image", methods=["GET"])
def get_image():
    """
    Endpoint to serve a full image from a dataset.

    Query Parameters:
        filepath (str): Path to the dataset file
        band (int, optional): Band index to read. Defaults to 1.

    Returns:
        PNG image of the entire dataset
    """
    try:
        filepath, band, time = validate_os_params()
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    return get_image_data(filepath, band, time)


@app.route("/bounds", methods=["GET"])
def get_bounds():
    try:
        filepath, band, time = validate_os_params()
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    return get_image_bounds(filepath, band, time)


@app.route("/", methods=["GET"])
@app.route("/<path:filepath>", methods=["GET"])
def serve_dist_directory(filepath=""):
    """
    Endpoint to serve files from the dist directory.

    Path Parameters:
        filepath (str): Relative path to the file within the dist directory

    Returns:
        The requested file if it exists, or a 404 error
    """
    try:
        # Get the dist directory path
        dist_dir = os.path.join(os.getcwd(), "earth_data_kit/viz_ui/dist")

        # If no specific file is requested, serve index.html
        if filepath == "":
            filepath = "index.html"

        return send_from_directory(dist_dir, filepath)
    except Exception as e:
        logger.error(f"Error serving file from dist directory: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Enable CORS for all routes
CORS(app, expose_headers="*")


def open_browser():
    INDEX_URL = "http://localhost:5432/status"

    while True:
        response = requests.get(url=INDEX_URL)
        if response.status_code == 200:
            break
        time.sleep(1)

    logger.info("Server started. Open http://localhost:5432/ to view the UI.")


def serve(source, band, time):
    os.environ["SOURCE"] = source
    os.environ["BAND"] = str(band)
    os.environ["TIME"] = str(time)
    threading.Thread(target=open_browser).start()

    app.run(host="0.0.0.0", port=5432)
