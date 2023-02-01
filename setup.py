import os
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

OPENCV_VERSION = os.getenv("OPENCV_VERSION")

install_requires = ['scikit-image', 'image', 'requests', 'boto3',
                    'numpy', 'matplotlib', 'pandas', 'kaggle', 'google-cloud-storage',
                    'dask[complete]<=2023.1.0', 'ipywidgets',
                    # Pinning this to be able to install tensorflow.
                    'protobuf<3.20.0',
                    # Pinning this to be able to install google-cloud-bigquery
                    'grpcio-status==1.48.2'
                    ]
if OPENCV_VERSION is None:
    install_requires.append('opencv-python')

setuptools.setup(
    name="aperturedb",
    version="0.4.0",
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
)
