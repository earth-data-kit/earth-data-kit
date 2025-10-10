class PlanetaryComputer:
    def __init__(self) -> None:
        self.name = "planetary_computer"

    # Write a sign function which sign any stac asset or stac item, either users EDK_AZURE_SAS_TOKEN if specified or uses planetary_computer.sign_* functions.
    def sign(self, url):
        # Authentcation is required for Planetary Computer. It can be supplied in multiple ways, eg: A function which can be used to sign internal STAC items/assets or via env variable EDK_AZURE_SAS_TOKEN. This variable is used by edk to sign requests to planetary computer by adding it in the URI via query parameter and using /vsicurl/ to access the file. User can also specify any of GDAL options which GDAL will automatically pick.
        pass

    def scan(self, source, time_opts, space_opts, tmp_path, band_locator):
        pass
        # User supplies a source which is a STAC collection url copied from planetary computer.

        # We run stac.scan function as most of the code is same. This can run without authentication. We save the urls and return to discover function.

    def sync(self, df, tmp_base_dir, overwrite=False):
        pass
