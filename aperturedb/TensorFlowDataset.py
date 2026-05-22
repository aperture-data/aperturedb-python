import math
import numpy as np
import cv2
import logging

from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

logger = logging.getLogger(__name__)


class ApertureDBTensorFlowDataset:
    """
    This class implements a TensorFlow Dataset for ApertureDB.
    It is used to load blobs returned by a `Find*` command from ApertureDB into a TensorFlow model.
    It can be initialized with a query that will be used to retrieve
    the blobs from ApertureDB. Note that only `FindImage` blobs are decoded via OpenCV.
    """

    def __init__(self, client: Connector, query, label_prop=None, batch_size=1, command_idx=None):

        self.client = client.clone()
        self.query = query
        self.command_idx = command_idx
        self.command_name = None
        self.total_elements = 0
        self.batch_size = batch_size
        self.batch_blobs = []
        self.batch_start = 0
        self.batch_end = 0
        self.label_prop = label_prop
        self.label_type = None

        allowed_find_commands = {"FindImage", "FindVideo", "FindBlob", "FindDescriptor", "FindBoundingBox"}

        if self.command_idx is not None:
            if not (0 <= self.command_idx < len(query)):
                raise ValueError(
                    f"command_idx {self.command_idx} is out of range.")
            self.command_name = list(query[self.command_idx].keys())[0]
            if self.command_name not in allowed_find_commands:
                raise ValueError(
                    f"Command at index {self.command_idx} is "
                    f"{self.command_name}, which is not a supported blob-returning Find* command.")
        else:
            for i in range(len(query)):
                name = list(query[i].keys())[0]
                if name in allowed_find_commands:
                    if self.command_idx is not None:
                        logger.warning(
                            "Multiple Find commands found. Selected %s at index %s.", self.command_name, self.command_idx)
                        break
                    self.command_idx = i
                    self.command_name = name

        if self.command_idx is None:
            msg = "Query error. The query must contain at least one supported blob-returning Find command (e.g., FindImage, FindVideo, FindBlob). The first one encountered will be used."
            logger.error(msg)
            raise ValueError(msg)

        if "results" not in self.query[self.command_idx][self.command_name]:
            self.query[self.command_idx][self.command_name]["results"] = {}

        if self.label_prop is not None:
            results = self.query[self.command_idx][self.command_name]["results"]
            if "list" not in results:
                results["list"] = []
            if self.label_prop not in results["list"]:
                results["list"].append(self.label_prop)

        for i in range(len(self.query)):
            name = list(self.query[i].keys())[0]
            if name.startswith("Find") and i != self.command_idx:
                self.query[i][name]["blobs"] = False

        self.query[self.command_idx][self.command_name]["batch"] = {}
        self.query[self.command_idx][self.command_name]["blobs"] = False

        try:
            _, r, b = execute_query(
                client=self.client, query=self.query, blobs=[])
            resp = r[self.command_idx][self.command_name]
            if resp.get("status", 0) != 0:
                raise Exception(
                    f"Query Error: {resp.get('status')} {resp.get('info', '')}")
            self.total_elements = resp.get("batch", {}).get("total_elements", resp.get("returned", 0))
        except:
            logger.error(
                f"Query error: {self.query} {self.client.get_last_response_str()}")
            raise
        finally:
            self.query[self.command_idx][self.command_name]["blobs"] = True

    def is_in_range(self, index):

        if index >= self.batch_start and index < self.batch_end:
            return True

        return False

    def get_batch(self, index):

        total_batches = math.ceil(self.total_elements / self.batch_size)
        batch_idx     = math.floor(index / self.batch_size)

        if batch_idx >= total_batches:
            raise Exception("Index out of range")

        query = self.query
        qbatch = query[self.command_idx][self.command_name].get("batch", {})
        qbatch["batch_size"] = self.batch_size
        qbatch["batch_id"] = batch_idx

        query[self.command_idx][self.command_name]["batch"] = qbatch

        try:

            # This is to handle potential issues with
            # disconnection/timeout and SSL context on multiprocessing
            connection_ok = False
            try:
                _, r, b = execute_query(
                    query=self.query, blobs=[], client=self.client)
                connection_ok = True
            except:
                # Connection failed, we retry just once to re-connect
                self.client = self.client.clone()

            if not connection_ok:
                # Connection failed, we have reconnected, we try again.
                _, r, b = execute_query(
                    query=self.query, blobs=[], client=self.client)

            resp = r[self.command_idx][self.command_name]
            if resp.get("status", 0) != 0:
                raise Exception(
                    f"Query Error: {resp.get('status')} {resp.get('info', '')}")

            if len(b) == 0:
                logger.error(f"index: {index}")
                raise Exception("No results returned from ApertureDB")

            self.batch_blobs = b
            self.batch_start = self.batch_size * batch_idx
            self.batch_end = self.batch_start + len(b)

            if self.label_prop:
                entities = r[self.command_idx][self.command_name]["entities"]
                try:
                    self.batch_labels = [l[self.label_prop] for l in entities]
                except KeyError:
                    prop = self.label_prop
                    msg = (
                        f"Property '{prop}' not found in some entities. "
                        "Ensure all entities have this property."
                    )
                    logger.error(msg)
                    raise
            else:
                self.batch_labels = ["none" for l in range(len(b))]
        except:
            logger.error(f"Query error: {self.client.get_last_response_str()}")
            raise

    def generator(self):
        for index in range(self.total_elements):
            if not self.is_in_range(index):
                self.get_batch(index)

            idx = index % self.batch_size
            blob = self.batch_blobs[idx]
            label = self.batch_labels[idx]

            if self.command_name == "FindImage":
                nparr = np.frombuffer(blob, dtype=np.uint8)
                blob = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if blob is None:
                    raise ValueError(f"Failed to decode image at index {index}.")
                blob = cv2.cvtColor(blob, cv2.COLOR_BGR2RGB)

            yield blob, label

    def get_dataset(self):
        import tensorflow as tf

        if self.label_type is None:
            if self.total_elements > 0:
                self.get_batch(0)
                if isinstance(self.batch_labels[0], int):
                    self.label_type = tf.int32
                elif isinstance(self.batch_labels[0], float):
                    self.label_type = tf.float32
                else:
                    self.label_type = tf.string
            else:
                self.label_type = tf.string

        if self.command_name == "FindImage":
            tensor_shape = (None, None, 3)
            tensor_dtype = tf.uint8
        else:
            tensor_shape = ()
            tensor_dtype = tf.string

        return tf.data.Dataset.from_generator(
            self.generator,
            output_signature=(
                tf.TensorSpec(shape=tensor_shape, dtype=tensor_dtype),
                tf.TensorSpec(shape=(), dtype=self.label_type)
            )
        )
