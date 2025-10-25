import logging
from earth_data_kit.stitching.engines.stac import STAC
import os
import requests

logger = logging.getLogger(__name__)

class PlanetaryComputer:
    """
    Planetary Computer engine with URL signing capabilities.
    Uses composition to leverage STAC functionality.
    """

    def __init__(self) -> None:
        self.name = "planetary_computer"
        self._stac = STAC()  # Composition: use STAC as a helper

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        """
        Scan Planetary Computer STAC catalog for available items.
        Delegates to STAC.scan() with URL signing enabled.
        """
        # Call STAC's scan method with sign_url as the url_signer
        df = self._stac.scan(
            source=source,
            time_opts=time_opts,
            space_opts=space_opts,
            tmp_path=tmp_path,
            band_locator=band_locator
        )

        return df

    def sync(self, df, tmp_base_dir, overwrite=True):
        """
        Sync (download) files from Planetary Computer to local storage.
        Delegates to STAC's sync method since the logic is the same.
        """
        return self._stac.sync(df, tmp_base_dir, overwrite)