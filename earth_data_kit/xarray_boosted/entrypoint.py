from xarray.backends import BackendEntrypoint
import os
def open_edk_dataset(filename_or_obj):
    pass

class EDKDatasetBackendEntrypoint(BackendEntrypoint):
    def open_dataset(
        self,
        filename_or_obj,
        *,
        drop_variables=None,
        # other backend specific keyword arguments
        # `chunks` and `cache` DO NOT go here, they are handled by xarray
    ):
        return open_edk_dataset(filename_or_obj)

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj):
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".my_format", ".my_fmt"}

    description = "Use .my_format files in Xarray"

    url = "https://link_to/your_backend/documentation"
