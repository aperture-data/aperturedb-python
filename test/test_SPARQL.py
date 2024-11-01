from pathlib import Path
import os
import subprocess
import runpy
import requests
import shutil
import pytest
import numpy as np
import pandas as pd
import os.path as osp
import tempfile
from aperturedb.Query import QueryBuilder, Query
from aperturedb.Entities import Entities
from aperturedb.Constraints import Constraints
from aperturedb.Images import Images
from aperturedb.Utils import Utils
from aperturedb.SPARQL import SPARQL
from aperturedb.cli.ingest import from_csv, TransformerType, IngestType
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.image_properties import ImageProperties
from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings

import logging
logger = logging.getLogger(__name__)


@pytest.fixture
def load_cookbook(utils: Utils, db):
    utils.remove_all_indexes()
    utils.remove_all_objects()

    temp_dir = tempfile.mkdtemp()
    # temp_path = Path(temp_dir)
    original_dir = os.getcwd()
    os.chdir(temp_dir)

    # Define the URL and file path for the script
    file_url = "https://raw.githubusercontent.com/aperture-data/Cookbook/refs/heads/main/scripts/convert_ingredients_adb_csv.py"
    file_path = Path("convert_ingredients_adb_csv.py")

    try:
        # Download the script file
        response = requests.get(file_url)
        file_path.write_text(response.text)

        runpy.run_path(str(file_path), run_name="__main__")

        data = ImageDataCSV("dishes.adb.csv")
        data = CLIPPyTorchEmbeddings(data, client=db)
        data = ImageProperties(data, client=db)
        data = CommonProperties(data, client=db)
        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=100, stats=True)

        data = EntityDataCSV("ingredients.adb.csv")
        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=100, stats=True)

        data = ConnectionDataCSV("dish_ingredients.adb.csv")
        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=100, stats=True)
    finally:
        os.chdir(original_dir)


# Tag the test functions that depend on the setup as external_network
def pytest_collection_modifyitems(items):
    for item in items:
        if "load_cookbook" in getattr(item, "fixturenames", ()):
            item.add_marker("external_network")

# Test functions that depends on the setup


@pytest.fixture
def sparql(db):
    sparql = SPARQL(db)
    print(sparql.schema)
    assert sparql.connections, f"No connections {sparql.schema}"
    assert sparql.properties, f"No properties {sparql.schema}"
    return sparql


@pytest.mark.parametrize("description,query", [
    ('Find all images with chicken and butter as ingredients',
     """
SELECT ?s ?caption {
?s c:HasIngredient [p:name "chicken"] , [p:name "butter"] ;
    p:caption ?caption .
} LIMIT 10
"""),
    ('Find all images with chicken or butter as ingredients',
     """
SELECT ?s ?caption WHERE {
VALUES ?ingredient { "chicken" "butter" }
?s c:HasIngredient [p:name ?ingredient] ;
    p:caption ?caption .
} LIMIT 10
"""),
    ('Find the top 10 ingredients',
     """
SELECT (COUNT(*) AS ?count) ?ingredient WHERE {
?s c:HasIngredient [p:name ?ingredient] .
} GROUP BY ?ingredient ORDER BY DESC(?count) LIMIT 10
"""),
    ('Do a descriptor search for a random image',
     f"""
SELECT ?i ?distance ?d ?caption WHERE {{
?d knn:similarTo [
    knn:set 'ViT-B/16' ;
    knn:k_neighbors 20 ;
    knn:vector "{SPARQL.encode_descriptor(np.random.rand(512))}" ;
    knn:distance ?distance
] ;
    c:ANY ?i . # Use fake connection because we can't say c:_DescriptorConnection
    ?i p:caption ?caption .
}}
""")])
def test_sparql(load_cookbook, sparql, query, description):
    results = sparql.query(query)
    assert results, f"No results for {description}"
