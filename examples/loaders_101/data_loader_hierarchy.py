"""
Data Loader Hierarchy and Examples

ApertureDB python SDK uses `ParallelLoader` as the main mechanism to efficiently batch and load data in parallel.
The `ParallelLoader` relies on the `Subscriptable` interface.
Classes that inherit from `Subscriptable` can be passed to `ParallelLoader.ingest()`.

The main data loaders provided by ApertureDB handle parsing CSVs and generating the appropriate ApertureDB queries.
The hierarchy is as follows:

Subscriptable
 └── CSVParser
      ├── EntityDataCSV (Loads entities and their properties)
      ├── ImageDataCSV (Loads images as entities and uploads image blobs)
      ├── VideoDataCSV (Loads videos)
      ├── BlobDataCSV (Loads generic blobs)
      ├── DescriptorDataCSV (Loads descriptors, e.g., embeddings)
      ├── ConnectionDataCSV (Loads connections between entities)
      ├── BBoxDataCSV (Loads bounding boxes)
      └── PolygonDataCSV (Loads polygons)

Other non-CSV data loaders (like PyTorchData, KaggleData) also inherit from `Subscriptable`.

The following script demonstrates how to instantiate different loaders and tie them together using `ParallelLoader`.
"""

import os
import base64
import tempfile
import pandas as pd
from aperturedb.Connector import Connector
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV


def create_sample_csvs(base_dir):
    # 1. Create a sample CSV for Entities (Persons)
    df_persons = pd.DataFrame({
        "EntityClass": ["Person", "Person"],
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    })
    persons_csv = os.path.join(base_dir, "persons.csv")
    df_persons.to_csv(persons_csv, index=False)

    # 2. Create a sample CSV for Images
    # Note: the paths should point to real images in a real scenario
    dummy_image_path = os.path.join(base_dir, "dummy_image.png")
    # 1x1 transparent PNG base64
    tiny_png_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
    with open(dummy_image_path, "wb") as f:
        f.write(base64.b64decode(tiny_png_b64))

    df_images = pd.DataFrame({
        "filename": [dummy_image_path, dummy_image_path],
        "image_id": ["img1", "img2"],
        "source": ["camera1", "camera2"]
    })
    images_csv = os.path.join(base_dir, "images.csv")
    df_images.to_csv(images_csv, index=False)

    # 3. Create a sample CSV for Connections (Person -> Image)
    df_connections = pd.DataFrame({
        "ConnectionClass": ["HasImage", "HasImage"],
        "Person@name": ["Alice", "Bob"],
        "_Image@image_id": ["img1", "img2"],
        "connection_property": ["owns", "likes"]
    })
    connections_csv = os.path.join(base_dir, "connections.csv")
    df_connections.to_csv(connections_csv, index=False)

    return persons_csv, images_csv, connections_csv


def main():
    # Connect to ApertureDB (Make sure your ApertureDB instance is running)
    db = Connector()

    # Initialize the ParallelLoader
    # The ParallelLoader handles the actual ingestion of queries produced by the Data Loader objects
    loader = ParallelLoader(db)

    with tempfile.TemporaryDirectory() as temp_dir:
        persons_csv, images_csv, connections_csv = create_sample_csvs(temp_dir)

        # 1. Load Entities
        print("Loading Entities...")
        # By providing the kwargs like `name`, `age`, we map the columns to properties.
        person_loader = EntityDataCSV(
            persons_csv, name="name", age="age")
        loader.ingest(person_loader)

        # 2. Load Images
        print("Loading Images...")
        image_loader = ImageDataCSV(
            images_csv, image_id="image_id", source="source")
        loader.ingest(image_loader)

        # 3. Load Connections
        print("Loading Connections...")
        connection_loader = ConnectionDataCSV(
            connections_csv,
            connection_property="connection_property"
        )
        loader.ingest(connection_loader)
        print("Done loading data!")


if __name__ == "__main__":
    main()
