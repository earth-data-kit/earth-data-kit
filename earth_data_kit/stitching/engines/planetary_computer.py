import logging
from earth_data_kit.stitching.engines.stac import STAC

logger = logging.getLogger(__name__)


def sign_url(item_or_url):
    """
    Sign URLs using Planetary Computer API.
    Simple signing using only the planetary_computer library.
    """
    try:
        import planetary_computer

        # Case 1: STAC item dict
        if isinstance(item_or_url, dict):
            signed_item = planetary_computer.sign(item_or_url)
            logger.debug("Signed STAC item")
            return signed_item

        # Case 2: URL string
        signed_url = planetary_computer.sign_url(item_or_url)
        logger.debug("Signed URL")
        return signed_url

    except ImportError:
        logger.error("planetary_computer library not available. Install with: pip install planetary-computer")
        return item_or_url
    except Exception as e:
        logger.error(f"Error signing: {e}")
        return item_or_url


class PlanetaryComputer:
    """
    Planetary Computer engine with URL signing capabilities.
    Uses composition to leverage STAC functionality.
    """

    def __init__(self) -> None:
        self.name = "planetary_computer"
        self._stac = STAC()  # Composition: use STAC as a helper

    def _parse_stac_url(self, source: str) -> tuple[str, str | None]:
        """Delegate to STAC's URL parsing."""
        return self._stac._parse_stac_url(source)

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        """
        Scan Planetary Computer STAC catalog for available items.
        Delegates to STAC.scan() with URL signing enabled.
        """
        # Call STAC's scan method with sign_url as the url_signer
        return self._stac.scan(
            source=source,
            time_opts=time_opts,
            space_opts=space_opts,
            tmp_path=tmp_path,
            band_locator=band_locator,
            url_signer=sign_url  # add URL signing
        )

    def sync(self, df, tmp_base_dir, overwrite=True):
        """
        Sync (download) files from Planetary Computer to local storage.
        Delegates to STAC's sync method since the logic is the same.
        """
        return self._stac.sync(df, tmp_base_dir, overwrite)