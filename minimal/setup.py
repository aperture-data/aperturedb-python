import setuptools
import sys
from os.path import dirname,abspath,join

sys.path.insert(1, dirname( abspath( join( dirname( __file__ ), ".." ))))
from shared_setup import PACKAGE_VERSION, minimal_install_requires


with open("README.md", "r") as fh:
    minimal_long_description = fh.read()

setuptools.setup(
    name="aperturedb-minimal",
    version=PACKAGE_VERSION,
    description="ApertureDB Client Module ( Minimal )",
    install_requires=minimal_install_requires,
    long_description=minimal_long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aperture-data/aperturedb-python",
    license="Apache",
    packages=setuptools.find_packages(),
    python_requires='>=2.6, !=3.0.*, !=3.1.*, !=3.2.*, <4',
    author="Luis Remis",
    author_email="luis@aperturedata.io",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
