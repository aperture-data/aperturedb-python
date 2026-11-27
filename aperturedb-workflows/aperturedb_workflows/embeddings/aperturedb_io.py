"""Embedding search spaces are descriptor classes with native vector indexes."""
from typing import Optional
import logging

from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector
from aperturedb.ServerCompatibility import preferred_vector_index
from aperturedb.Utils import Utils

logger = logging.getLogger(__name__)


def _execute(client, query):
    status, response, _ = execute_query(client, query)
    if status != 0:
        raise RuntimeError(f"Vector index query failed: {response}")
    return response


def _descriptor_index(client: Connector, descriptor_set: str, engine: Optional[str]):
    indexes = Utils(client).get_vector_indexes(descriptor_set)
    return preferred_vector_index(indexes, engine)


def find_descriptor_set(client: Connector, descriptor_set: str,
                        engine: Optional[str] = None) -> Optional[dict]:
    """Return model metadata from the selected search index."""
    index = _descriptor_index(client, descriptor_set, engine)
    if index is None:
        return None
    metadata = dict(index.get("metadata") or {})
    for name in ("embeddings_provider", "embeddings_model", "embeddings_pretrained",
                 "embeddings_fingerprint"):
        metadata.setdefault(name, None)
    return metadata


def add_descriptor_set(client: Connector, descriptor_set: str, metric: str,
                       dimensions: int, engine: str,
                       properties: Optional[dict] = None) -> None:
    """Create a vector index, keeping customer model metadata separate."""
    _execute(client, [{"CreateIndex": {
        "target": "descriptor", "class": descriptor_set,
        "property": "_blob", "kind": engine.lower(),
        "params": {"dimensions": dimensions, "metric": metric},
        "metadata": properties or {}}}])


def update_descriptor_set(client: Connector, descriptor_set: str,
                          properties: dict, engine: Optional[str] = None) -> None:
    """Merge metadata without dropping fingerprint/model fields."""
    index = _descriptor_index(client, descriptor_set, engine)
    if index is None:
        raise ValueError(f"No vector index for {descriptor_set}")
    metadata = {**index.get("metadata", {}), **properties}
    _execute(client, [{"UpdateIndex": {
        "target": "descriptor", "class": descriptor_set,
        "property": "_blob", "kind": index["kind"], "metadata": metadata}}])


def delete_descriptor_set(client: Connector, descriptor_set: str) -> None:
    """Remove descriptors, indexes and dependent connection categories atomically."""
    if not Utils(client).remove_descriptorset(descriptor_set):
        raise RuntimeError(
            f"Could not remove descriptor class {descriptor_set}")
