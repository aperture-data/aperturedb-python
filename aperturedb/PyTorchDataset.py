import os
import math

import numpy as np
import cv2

from aperturedb import Images

import torch
from torch.utils import data
from torchvision import transforms

class ApertureDBDatasetConstraints(data.Dataset):

    # initialise function of class
    def __init__(self, db, constraints):

        self.imgs_handler = Images.Images(db)
        self.imgs_handler.search(constraints=constraints)

    def __getitem__(self, index):

        if index >= self.imgs_handler.total_results():
            raise StopIteration

        img = self.imgs_handler.get_np_image_by_index(index)

        # This is temporary until we define a good, generic way, of
        # retriving a label associated with the image.
        label = "none"
        return img, label

    def __len__(self):

        return self.imgs_handler.total_results()

class ApertureDBDataset(data.Dataset):

    # initialise function of class
    def __init__(self, db, query, label_prop=None):

        self.db = db
        self.query = query
        self.find_image_idx = None
        self.total_elements = 0
        self.batch_size     = 100
        self.batch_images   = []
        self.batch_start    = 0
        self.batch_end      = 0
        self.label_prop     = label_prop

        for i in range(len(query)):

            name = list(query[i].keys())[0]
            if name == "FindImage":
                self.find_image_idx = i

        if self.find_image_idx is None:
            print("Query error. The query must containt one FindImage command")
            raise Exception('Query Error')

        if not "results" in self.query[self.find_image_idx]["FindImage"]:
            self.query[self.find_image_idx]["FindImage"]["results"] = {}

        self.query[self.find_image_idx]["FindImage"]["batch"] = {}

        try:
            r,b = self.db.query(self.query)
            batch = r[self.find_image_idx]["FindImage"]["batch"]
            self.total_elements = batch["total_elements"]
        except:
            print("Query error:")
            print(self.query)
            print(self.db.get_last_response_str())
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

        query  = self.query
        qbatch = query[self.find_image_idx]["FindImage"]["batch"]
        qbatch["batch_size"] = self.batch_size
        qbatch["batch_id"]   = batch_idx

        query[self.find_image_idx]["FindImage"]["batch"] = qbatch

        try:
            r,b = self.db.query(query)
            if len(b) == 0:
                print("index:", index)
                raise Exception("No results returned from ApertureDB")

            self.batch_images = b
            self.batch_start  = self.batch_size * batch_idx
            self.batch_end    = self.batch_start + len(b)

            if self.label_prop:
                entities = r[self.find_image_idx]["FindImage"]["entities"]
                self.batch_labels = [ l[self.label_prop] for l in entities]
            else:
                self.batch_labels = [ "none" for l in range(len(b))]
        except:
            print("Query error:")
            print(self.db.get_last_response_str())
            raise
