import os
import logging

logger = logging.getLogger(__name__)

class EarthEngine:
    def __init__(self) -> None:
        self.app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT")

    def create_inventory(self, patterns, time_opts, space_opts, tmp_base_dir):
        print (self, patterns, time_opts, space_opts)
        pass

    def sync_inventory(self, df, tmp_base_dir):
        pass
