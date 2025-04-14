import pandas as pd
import earth_data_kit as edk
import logging
from flask import Flask, send_from_directory
import os
import datetime
import time
import webbrowser

logger = logging.getLogger(__name__)


class DeckGL:
    def __init__(self, da):
        self.da = da

    def plot(self):
        source = self.da.attrs["source"]
        time_value = pd.to_datetime(str(self.da.coords["time"].values)).strftime(
            "%Y-%m-%d-%H:%M:%S"
        )
        band_value = self.da.coords["band"].values

        edk.viz_server.serve(source, band_value, time_value)
