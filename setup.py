import os
import setuptools

PACKAGE_VERSION="0.4.6"
with open("README.md", "r") as fh:
    long_description = fh.read()

with open("minimal/README.md", "r") as fh:
    minimal_long_description = fh.read()

OPENCV_VERSION = os.getenv("OPENCV_VERSION")

minimal_install_requires = [
                    # Pin to the bridge version.
                    # https://github.com/tensorflow/tensorflow/issues/60320
                    'protobuf==3.20.3'
                   ]
install_requires = ['scikit-image', 'image', 'requests', 'boto3',
                    'numpy', 'matplotlib', 'pandas', 'kaggle', 'google-cloud-storage',
                    'dask[complete]', 'ipywidgets', 'pydantic', 'devtools', 'typer[all]',
                    # Pinning this to be able to install google-cloud-bigquery
                    'grpcio-status==1.48.2',
                    # Pinning this to resolve test errors temporarily
                    'ipywidgets==8.0.4'
                    ] + minimal_install_requires
if OPENCV_VERSION is None:
    install_requires.append('opencv-python')

setuptools.setup(
    name="aperturedb",
    version=PACKAGE_VERSION,
    description="ApertureDB Client Module",
    install_requires=install_requires,
    long_description=long_description,
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
    entry_points = {
        'console_scripts': ['adb=aperturedb.cli.adb:app'],
    }
)

os.chdir( "./minimal" )
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
