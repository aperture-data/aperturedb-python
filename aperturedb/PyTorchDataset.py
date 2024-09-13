import math
import numpy as np
import cv2
import logging

from torch.utils import data

from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector


logger = logging.getLogger(__name__)


class ApertureDBDataset(data.Dataset):
    """
    This class implements a PyTorch Dataset for ApertureDB.
    It is used to load images from ApertureDB into a PyTorch model.
    It can be initialized with a query that will be used to retrieve
    the images from ApertureDB.
    """

    def __init__(self, client: Connector, query, label_prop=None, batch_size=1):

        self.client = client.clone()
        self.query = query
        self.find_image_idx = None
        self.total_elements = 0
        self.batch_size     = batch_size
        self.batch_images   = []
        self.batch_start    = 0
        self.batch_end      = 0
        self.label_prop     = label_prop

        for i in range(len(query)):

            name = list(query[i].keys())[0]
            if name == "FindImage":
                self.find_image_idx = i

        if self.find_image_idx is None:
            logger.error(
                "Query error. The query must contain one FindImage command")
            raise Exception('Query Error')

        if not "results" in self.query[self.find_image_idx]["FindImage"]:
            self.query[self.find_image_idx]["FindImage"]["results"] = {}

        self.query[self.find_image_idx]["FindImage"]["batch"] = {}
        self.query[self.find_image_idx]["FindImage"]["blobs"] = True

        try:
            _, r, b = execute_query(
                client=self.client, query=self.query, blobs=[])
            batch = r[self.find_image_idx]["FindImage"]["batch"]
            self.total_elements = batch["total_elements"]
        except:
            logger.error(
                f"Query error: {self.query} {self.client.get_last_response_str()}")
            raise

    def __getitem__(self, index):

        if index >= self.total_elements:
            raise StopIteration

        if not self.is_in_range(index):
            self.get_batch(index)

        idx = index % self.batch_size
        img   = self.batch_images[idx]
        label = self.batch_labels[idx]

        nparr = np.frombuffer(img, dtype=np.uint8)
        img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        img   = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        return img, label

    def __len__(self):

        return self.total_elements

    def is_in_range(self, index):

        if index >= self.batch_start and index < self.batch_end:
            return True

        return False

    def get_batch(self, index):

        total_batches = math.ceil(self.total_elements / self.batch_size)
        batch_idx     = math.floor(index / self.batch_size)

        if batch_idx >= total_batches:
            raise Exception("Index out of range")

        query  = self.query
        qbatch = query[self.find_image_idx]["FindImage"]["batch"]
        qbatch["batch_size"] = self.batch_size
        qbatch["batch_id"]   = batch_idx

        query[self.find_image_idx]["FindImage"]["batch"] = qbatch

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

            if len(b) == 0:
                logger.error(f"index: {index}")
                raise Exception("No results returned from ApertureDB")

            self.batch_images = b
            self.batch_start  = self.batch_size * batch_idx
            self.batch_end    = self.batch_start + len(b)

            if self.label_prop:
                entities = r[self.find_image_idx]["FindImage"]["entities"]
                self.batch_labels = [l[self.label_prop] for l in entities]
            else:
                self.batch_labels = ["none" for l in range(len(b))]
        except:
            logger.error(f"Query error: {self.client.get_last_response_str()}")
            raise
