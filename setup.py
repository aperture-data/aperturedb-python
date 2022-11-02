import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aperturedb",
    version="0.3.4",
    description="ApertureDB Client Module",
    install_requires=['protobuf>=3.20.0', 'scikit-image', 'image', 'requests', 'boto3',
                      'opencv-python', 'numpy', 'matplotlib', 'pandas', 'kaggle', 'google-cloud-storage',
                      'dask[complete]'],
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
