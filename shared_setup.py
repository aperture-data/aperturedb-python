import os

PACKAGE_VERSION="0.4.6"

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
