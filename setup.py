from setuptools import setup, find_packages

VERSION = "1.0.0-alpha"
DESCRIPTION = "Making GIS data analysis cheap and easy"
LONG_DESCRIPTION = """Contains a set of modules and tools to make geospatial data analysis easy and cheap. 
Aims to fill the gaps integrating different open-source modules"""

# Setting up
setup(
    # the name must match the folder name 'spacetime_tools'
    name="spacetime_tools",
    version=VERSION,
    author="Siddhant Gupta",
    author_email="<siddhantgupta3@gmail.com>",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[],  # add any additional packages that
    # needs to be installed along with your package.
    keywords=["python", "gis", "earth data"],
    classifiers=[
        "Private :: Do Not Upload",
        "Development Status :: 2 - Pre-Alpha" "Intended Audience :: Education",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
    ],
)
