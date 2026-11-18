"""LangChain vector store for ApertureDB.

Install ``aperturedb[langchain]`` to use this optional integration. Documents are stored in a descriptor class with a vector index.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from aperturedb.CommonLibrary import create_connector
from aperturedb.Constraints import Constraints, predicate
from aperturedb.Descriptors import Descriptors
from aperturedb.DescriptorEngines import select_engine
from aperturedb.ServerCompatibility import descriptor_selector, preferred_vector_index, typed_server
from aperturedb.Utils import Utils


class ApertureDB(VectorStore):
    """Store LangChain documents in a named descriptor class.

    ``descriptor_set`` is an alias for ``descriptor_class``. Pass an existing client
    to reuse its configuration and session, or use the normal SDK profile.
    """

    def __init__(self, embeddings: Embeddings, descriptor_class: str = "langchain",
                 dimensions: Optional[int] = None, engine: Optional[str] = None,
                 metric: Optional[str] = None, properties: Optional[dict] = None,
                 client=None, descriptor_set: Optional[str] = None,
                 batch_size: int = 1000, **kwargs: Any):
        if embeddings is None:
            raise ValueError("An embeddings implementation is required")
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if descriptor_set is not None:
            if descriptor_class != "langchain" and descriptor_class != descriptor_set:
                raise ValueError(
                    "descriptor_class and descriptor_set disagree")
            descriptor_class = descriptor_set
        self.descriptor_class = descriptor_class
        self.embedding_function = embeddings
        self.connection = client if client is not None else create_connector()
        self.utils = Utils(self.connection)
        self.batch_size = batch_size
        self._modern = typed_server(self.connection)
        if self._modern:
            definitions = self.utils.get_vector_indexes(descriptor_class)
            definition = preferred_vector_index(definitions, engine, metric)
            if definitions and definition is None:
                raise ValueError(
                    "Requested engine or metric is absent from the vector indexes")
            params = definition["params"] if definition else None
            configured_engines = {definition["kind"]: {}} if definition else {}
            metadata = definition.get("metadata", {}) if definition else {}
        else:
            response, _ = self.utils.execute([{"FindDescriptorSet": {
                "with_name": descriptor_class, "dimensions": True,
                "engines": True, "metrics": True, "results": {"all_properties": True}}}])
            rows = response[0]["FindDescriptorSet"].get("entities", [])
            row = rows[0] if rows else None
            params = {"dimensions": row["_dimensions"],
                      "metric": row["_metrics"]} if row else None
            metadata = row or {}
            configured_engines = row["_engines"] if row else {}
        if params is not None:
            names = list(configured_engines) if isinstance(
                configured_engines, dict) else configured_engines
            if engine and engine.casefold() not in {name.casefold() for name in names}:
                raise ValueError(
                    "Requested engine is absent from the vector index")
            metrics = params["metric"]
            metrics = [metrics] if isinstance(metrics, str) else metrics
            if dimensions is not None and dimensions != params["dimensions"]:
                raise ValueError(
                    "Embedding dimensions differ from the stored vector index")
            if metric is not None and metric not in metrics:
                raise ValueError(
                    "Requested metric is absent from the vector index")
            self.dimensions = params["dimensions"]
            self.engine = engine or select_engine(configured_engines)
            self.metric = metric or metrics[0]
            self.properties = {name[3:]: value for name, value in metadata.items()
                               if name.startswith("lc_")}
            for name, value in (properties or {}).items():
                if self.properties.get(name) != value:
                    raise ValueError(
                        "Vector-space metadata differs for " + name)
        else:
            self.dimensions = dimensions if dimensions is not None else len(
                embeddings.embed_query("dimensions"))
            self.engine = engine or "HNSW"
            self.metric = metric or "CS"
            self.properties = properties or {}
            if not self.utils.create_vector_index(descriptor_class, self.dimensions,
                                                  self.metric, [self.engine],
                                                  {"lc_" + key: value for key, value in self.properties.items()}):
                raise RuntimeError(self.utils.client.get_last_response_str())
        selector = "descriptor:" + descriptor_class if self._modern else "_Descriptor"
        if not self.utils.create_entity_index(selector, "uniqueid"):
            raise RuntimeError(self.utils.client.get_last_response_str())

    @property
    def embeddings(self) -> Embeddings:
        return self.embedding_function

    def _execute(self, query, blobs=None):
        # Default LangChain async methods can call these operations from worker
        # threads. Each operation owns its socket while sharing session state.
        operation = Utils(self.connection)
        try:
            return operation.execute(query, blobs or [])
        finally:
            operation.client.close()

    def _document(self, row):
        return Document(page_content=row["text"], id=row["uniqueid"],
                        metadata={key[3:]: value for key, value in row.items() if key.startswith("lc_")})

    def add_texts(self, texts: Iterable[str], metadatas=None, ids=None, **kwargs: Any):
        texts = list(texts)
        if not texts:
            return []
        metadatas = list(metadatas) if metadatas is not None else [
            {} for _ in texts]
        ids = [str(uuid4()) if value is None else value for value in ids] if ids is not None else [
            str(uuid4()) for _ in texts]
        if len(metadatas) != len(texts) or len(ids) != len(texts):
            raise ValueError(
                "texts, metadatas and ids must have matching lengths")
        if any(not isinstance(value, str) or not value for value in ids) or len(set(ids)) != len(ids):
            raise ValueError("Document IDs must be unique nonempty strings")
        vectors = np.asarray(
            self.embedding_function.embed_documents(texts), dtype=np.float32)
        if vectors.shape != (len(texts), self.dimensions) or not np.isfinite(vectors).all():
            raise ValueError(
                "Embeddings must be finite vectors with the configured dimensions")
        for start in range(0, len(texts), self.batch_size):
            stop = min(start + self.batch_size, len(texts))
            query = [{"DeleteDescriptor": {**descriptor_selector(self.connection, self.descriptor_class),
                      "constraints": predicate("uniqueid", "in", ids[start:stop])}}]
            for text, metadata, identifier in zip(texts[start:stop], metadatas[start:stop], ids[start:stop]):
                properties = {"lc_" + key: ({"_json": value} if value is None or isinstance(value, (dict, list)) else value)
                              for key, value in metadata.items()}
                properties.update(text=text, uniqueid=identifier)
                query.append({"AddDescriptor": {
                    "class" if self._modern else "set": self.descriptor_class, "properties": properties}})
            self._execute(query, [vector.tobytes()
                          for vector in vectors[start:stop]])
        return ids

    @classmethod
    def from_texts(cls, texts, embedding, metadatas=None, **kwargs: Any):
        ids = kwargs.pop("ids", None)
        store = cls(embeddings=embedding, **kwargs)
        store.add_texts(texts, metadatas, ids=ids)
        return store

    def get_by_ids(self, ids, /):
        if not ids:
            return []
        response, _ = self._execute([{"FindDescriptor": {
            **descriptor_selector(self.connection, self.descriptor_class),
            "constraints": predicate("uniqueid", "in", list(ids)),
            "results": {"all_properties": True}}}])
        return [self._document(row) for row in response[0]["FindDescriptor"].get("entities", [])]

    def delete(self, ids=None, **kwargs: Any):
        if ids is None:
            raise ValueError(
                "Provide document IDs; use delete_vectorstore to remove the whole class")
        if ids:
            self._execute([{"DeleteDescriptor": {
                **descriptor_selector(self.connection, self.descriptor_class),
                "constraints": predicate("uniqueid", "in", list(ids))}}])
        return True

    def _search(self, vector, k, filter=None, mmr=False, fetch_k=20, lambda_mult=0.5):
        operation = self.connection.clone()
        try:
            descriptors = Descriptors(operation)
            constraints = Constraints()
            if filter is not None:
                if isinstance(filter, Constraints):
                    constraints = filter
                elif isinstance(filter, list):
                    constraints = SimpleNamespace(constraints=filter)
                else:
                    raise ValueError(
                        "filter must be a Constraints instance or an expression array")
            options = dict(constraints=constraints,
                           engine=self.engine, metric=self.metric)
            if mmr:
                descriptors.find_similar_mmr(self.descriptor_class, vector, k, fetch_k,
                                             lambda_mult=lambda_mult, **options)
            else:
                descriptors.find_similar(
                    self.descriptor_class, vector, k, distances=True, **options)
            return descriptors.response
        finally:
            operation.close()

    def similarity_search_with_score(self, query, k=4, **kwargs: Any):
        rows = self._search(self.embeddings.embed_query(
            query), k, filter=kwargs.get("filter"))
        return [(self._document(row), row["_distance"]) for row in rows]

    def similarity_search(self, query, k=4, **kwargs: Any):
        return [doc for doc, score in self.similarity_search_with_score(query, k, **kwargs)]

    def similarity_search_by_vector(self, embedding, k=4, **kwargs: Any):
        return [self._document(row) for row in self._search(embedding, k, filter=kwargs.get("filter"))]

    def max_marginal_relevance_search_by_vector(self, embedding, k=4, fetch_k=20,
                                                lambda_mult=0.5, **kwargs: Any):
        rows = self._search(embedding, k, filter=kwargs.get("filter"), mmr=True,
                            fetch_k=fetch_k, lambda_mult=lambda_mult)
        return [self._document(row) for row in rows]

    def max_marginal_relevance_search(self, query, k=4, fetch_k=20, lambda_mult=0.5, **kwargs: Any):
        return self.max_marginal_relevance_search_by_vector(self.embeddings.embed_query(query),
                                                            k=k, fetch_k=fetch_k, lambda_mult=lambda_mult, **kwargs)

    def _select_relevance_score_fn(self):
        if self.metric == "CS":
            # Native CS returns a similarity; legacy releases expose the same
            # configured score through the descriptor helper.
            return lambda score: (score + 1.0) / 2.0
        if self.metric == "L2":
            return lambda distance: 1.0 / (1.0 + distance)
        raise ValueError(
            "Score thresholds need normalized CS or L2; use similarity search for IP")

    @classmethod
    def delete_vectorstore(cls, descriptor_class, *, client=None):
        utils = Utils(client if client is not None else create_connector())
        if not utils.remove_descriptorset(descriptor_class):
            raise RuntimeError(utils.client.get_last_response_str())

    @classmethod
    def list_vectorstores(cls, *, client=None):
        utils = Utils(client if client is not None else create_connector())
        if typed_server(utils.client):
            return utils.get_vector_indexes()
        response, _ = utils.execute([{"FindDescriptorSet": {"results": {"all_properties": True},
                                                            "engines": True, "metrics": True, "dimensions": True}}])
        return response[0]["FindDescriptorSet"].get("entities", [])
