import logging

import numpy as np

from aperturedb.Entities import Entities
from aperturedb.CommonLibrary import execute_query

logger = logging.getLogger(__name__)


class Descriptors(Entities):
    """
    Python wrapper for ApertureDB Descriptors API.
    """
    db_object = "_Descriptor"

    def find_similar(
        self,
        set: str,
        vector,
        k_neighbors: int,
        constraints=None,
        distances: bool = False,
        blobs: bool = False,
        results={"all_properties": True},
    ):
        """
        Find similar descriptor sets to the input descriptor set.

        Args:
            set (str): Descriptor set name.
            vector (list): Input descriptor set vector.
            k_neighbors (int): Number of neighbors to return.
            distances (bool): Return similarity metric values.
            blobs (bool): Return vectors of the neighbors.
            results (dict): Dictionary with the results format.
                Defaults to all properties.

        Returns:
            results: Response from the server.
        """

        command = {
            "FindDescriptor": {
                "set": set,
                "distances": distances,
                "blobs": blobs,
                "results": results,
                "k_neighbors": k_neighbors,
            }
        }

        if constraints is not None:
            command["FindDescriptor"]["constraints"] = constraints.constraints

        query = [command]
        blobs_in = [np.array(vector, dtype=np.float32).tobytes()]
        _, response, blobs_out = execute_query(self.client, query, blobs_in)

        self.response = response[0]["FindDescriptor"].get("entities", [])

        if blobs:
            for i, entity in enumerate(self.response):
                entity["vector"] = np.frombuffer(
                    blobs_out[i], dtype=np.float32)

    def _descriptorset_metric(self, set: str):
        """Find default metric for descriptor set"""
        command = {"FindDescriptorSet": {"with_name": set, "metrics": True}}
        query = [command]
        _, response, _ = execute_query(self.client, query, [])
        logger.debug(response)
        assert self.client.last_query_ok(), response
        return response[0]["FindDescriptorSet"]["entities"][0]["_metrics"][0]

    def _vector_similarity(self, v1, v2):
        """Find similarity between two vectors using the metric of the descriptor set."""
        if self.metric == "CS":
            return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        elif self.metric == "L2":
            # negate to turn distance into similarity
            return -np.linalg.norm(v1 - v2)
        elif self.metric == "IP":
            return np.dot(v1, v2)
        else:
            raise ValueError("Unknown metric: %s" % self.metric)

    def find_similar_mmr(
        self,
        set: str,
        vector,
        k_neighbors: int,
        fetch_k: int,
        lambda_mult: float = 0.5,
        **kwargs,
    ):
        """
        As find_similar, but using the MMR algorithm to diversify the results.

        Args:

            set (str): Descriptor set name.
            vector (list): Input descriptor set vector.
            k_neighbors (int): Number of results to return.
            fetch_k (int): Number of neighbors to fetch from the database.
            lambda_mult (float): Lambda multiplier for the MMR algorithm.
                Defaults to 0.5.  1.0 means no diversity.
        """
        self.metric = self._descriptorset_metric(set)
        vector = np.array(vector, dtype=np.float32)

        kwargs["blobs"] = True  # force vector return
        self.find_similar(set, vector, fetch_k, **kwargs)

        # MMR algorithm
        # Calculate similarity between query and all documents
        query_similarity = [self._vector_similarity(
            vector, d["vector"]) for d in self]
        # Calculate similarity between all pairs of documents
        document_similarity = {}
        for i, d in enumerate(self):
            for j, d2 in enumerate(self[i + 1:], i + 1):
                similarity = self._vector_similarity(d["vector"], d2["vector"])
                document_similarity[(i, j)] = similarity
                document_similarity[(j, i)] = similarity

        # We just gather indexes here, not the actual entities
        selected = []
        unselected = list(range(len(self)))

        while len(selected) < k_neighbors and unselected:
            if not selected:
                selected.append(0)
                unselected.remove(0)
            else:
                selected_unselected_similarity = np.array(
                    [
                        [document_similarity[(i, j)] for j in unselected]
                        for i in selected
                    ]
                )
                worst_similarity = np.max(
                    selected_unselected_similarity, axis=0)
                relevance_scores = np.array(
                    [query_similarity[i] for i in unselected])
                scores = (
                    1 - lambda_mult
                ) * worst_similarity + lambda_mult * relevance_scores
                max_index = unselected[np.argmax(scores)]
                selected.append(max_index)
                unselected.remove(max_index)
        logger.info("Selected indexes: %s; unselected %s",
                    selected, unselected)
        self.response = [self[i] for i in selected]
