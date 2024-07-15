class Tile:
    def __init__(self, remote_path, gdal_path) -> None:
        self.remote_path = remote_path
        self.gdal_path = gdal_path

    def get_metadata(self):
        pass

    def set_local_path(self, local_path):
        self.local_path = local_path
