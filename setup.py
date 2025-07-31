from setuptools import setup, find_packages
import sys
import platform
import os
import tempfile
import tarfile
import urllib.request
import subprocess
from wheel.bdist_wheel import bdist_wheel

__version__ = "0.1.3.dev20250630"


def get_platform_and_arch():
    return (sys.platform, platform.machine())

def get_gdal_version():
    try:
        result = subprocess.run(
            ["gdal-config", "--version"], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except Exception as e:
        raise e


gdal_ver = get_gdal_version()

dependencies = [
    "geopandas~=0.14.4",
    "python-dotenv~=1.0.1",
    "zarr~=2.18.2",
    "netcdf4~=1.7.1.post2",
    "earthengine-api~=1.5.2",
    "python-levenshtein~=0.26.1",
    "lxml~=5.3.1",
    "folium~=0.19.5",
    "matplotlib~=3.10.1",
    "xarray~=2025.1.2",
    "dask~=2025.2.0",
    "tqdm~=4.67.1",
    "rio-tiler~=7.6.0",
    "tenacity~=9.1.2",
    f"gdal=={gdal_ver}",
]

dev_dependencies = [
    "ipykernel~=6.29.4",
    "pytest~=8.3.2",
    "sphinx~=7.4.0",
    "furo~=2024.5.6",
    "pytest-xdist~=3.6.1",
    "black~=25.1.0",
    "sphinx-copybutton~=0.5.2",
    "rasterio~=1.4.3",
    "rioxarray~=0.18.2",
    "pyarrow~=19.0.1",
    "ipycytoscape~=1.3.3",
    "gcsfs~=2025.3.0",
    "dask[distributed]~=2025.3.0",
    "bokeh~=3.7.0",
    "pytest-order~=1.3.0",
]

class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False  # Mark as platform-specific

    def get_tag(self):
        # This will return something like ('cp312', 'cp312', 'macosx_10_9_x86_64')
        return super().get_tag()
    
setup(
    name="earth-data-kit",
    version=__version__,
    description="Making GIS data analysis cheap and easy",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Siddhant Gupta",
    author_email="siddhantgupta3@gmail.com",
    license="Apache-2.0",
    include_package_data=False,
    zip_safe=False,
    cmdclass={"bdist_wheel": CustomBdistWheel},
    package_data={
        "earth_data_kit": [
            "s5cmd/*",
        ],
    },
    packages=find_packages(include=["earth_data_kit", "earth_data_kit.*"]),
    python_requires=">=3.12",
    install_requires=dependencies,
    extras_require={
        "dev": dev_dependencies,
    },
    entry_points={
        "xarray.backends": [
            "edk_dataset = earth_data_kit.xarray_boosted.entrypoint:EDKDatasetBackend"
        ]
    },
)
