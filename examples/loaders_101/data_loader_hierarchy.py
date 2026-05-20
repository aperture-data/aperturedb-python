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
import pandas as pd
from aperturedb.Connector import Connector
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV


def create_sample_csvs():
    # 1. Create a sample CSV for Entities (Persons)
    df_persons = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    })
    df_persons.to_csv("persons.csv", index=False)

    # 2. Create a sample CSV for Images
    # Note: the paths should point to real images in a real scenario
    with open("dummy_image.jpg", "wb") as f:
        f.write(b"dummy_image_data")

    df_images = pd.DataFrame({
        "url": ["dummy_image.jpg", "dummy_image.jpg"],
        "source": ["camera1", "camera2"]
    })
    df_images.to_csv("images.csv", index=False)

    # 3. Create a sample CSV for Connections (Person -> Image)
    df_connections = pd.DataFrame({
        "src_name": ["Alice", "Bob"],
        "dst_url": ["dummy_image.jpg", "dummy_image.jpg"],
        "connection_property": ["owns", "likes"]
    })
    df_connections.to_csv("connections.csv", index=False)


def main():
    create_sample_csvs()

    # Connect to ApertureDB (Make sure your ApertureDB instance is running)
    db = Connector()

    # Initialize the ParallelLoader
    # The ParallelLoader handles the actual ingestion of queries produced by the Data Loader objects
    loader = ParallelLoader(db)

    # 1. Load Entities
    print("Loading Entities...")
    # By providing the kwargs like `name`, `age`, we map the columns to properties.
    person_loader = EntityDataCSV(
        "persons.csv", entity_class="Person", name="name", age="age")
    loader.ingest(person_loader)

    # 2. Load Images
    print("Loading Images...")
    image_loader = ImageDataCSV("images.csv", source="source")
    loader.ingest(image_loader)

    # 3. Load Connections
    print("Loading Connections...")
    connection_loader = ConnectionDataCSV(
        "connections.csv",
        connection_class="HasImage",
        src_class="Person",
        dst_class="Image",
        src_prop_key="name",     # Match the 'name' column in persons to 'src_name'
        src_prop_val="src_name",
        dst_prop_key="url",      # Match the 'url' column in images to 'dst_url'
        dst_prop_val="dst_url",
        connection_property="connection_property"
    )
    loader.ingest(connection_loader)
    print("Done loading data!")

    # Cleanup local files
    os.remove("persons.csv")
    os.remove("images.csv")
    os.remove("connections.csv")
    os.remove("dummy_image.jpg")


if __name__ == "__main__":
    main()
