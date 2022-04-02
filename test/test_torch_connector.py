import time
import unittest
import os

import torch
import torch.distributed as dist

from test_Base import TestBase

from aperturedb import Connector, Utils
from aperturedb import Images
from aperturedb import PyTorchDataset


class TestTorchDatasets(TestBase):

    '''
        These tests need to be run after the Loaders, because it uses
        data inserted by the loaders.
        TODO: We should change this so that we do not depend on them
    '''

    def test_omConstraints(self):

        db = self.create_connection()

        const = Images.Constraints()
        const.greaterequal("age", 0)
        dataset = PyTorchDataset.ApertureDBDatasetConstraints(
            db, constraints=const)

        dbutils = Utils.Utils(db)
        self.assertEqual(len(dataset), dbutils.count_images())

        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("\n")
        print("Throughput (imgs/s):", len(dataset) / (time.time() - start))

    def test_nativeContraints(self):

        db = self.create_connection()

        query = [{
            "FindImage": {
                "constraints": {
                    "age": [">=", 0]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": 224,
                        "height": 224
                    }
                ],
                "results": {
                    "list": ["license"]
                }
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(
            db, query, label_prop="license")

        dbutils = Utils.Utils(db)
        self.assertEqual(len(dataset), dbutils.count_images())

        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("\n")
        print("Throughput (imgs/s):", len(dataset) / (time.time() - start))

    def test_datasetWithMultiprocessing(self):

        db = self.create_connection()

        query = [{
            "FindImage": {
                "constraints": {
                    "age": [">=", 0]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": 224,
                        "height": 224
                    }
                ],
                "results": {
                    "list": ["license"]
                }
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(
            db, query, label_prop="license")

        dbutils = Utils.Utils(db)
        self.assertEqual(len(dataset), dbutils.count_images())

        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("\n")
        print("Sequential Throughput (imgs/s):",
              len(dataset) / (time.time() - start))

        # Distributed Data Loader Setup

        # Needed for init_process_group
        os.environ['MASTER_ADDR'] = 'localhost'
        os.environ['MASTER_PORT'] = '12355'

        dist.init_process_group("gloo", rank=0, world_size=1)

        # === Distributed Data Loader Sequential

        data_loader = torch.utils.data.DataLoader(
            dataset,
            batch_size=10,          # pick random values here to test
            num_workers=4,          # num_workers > 1 to test multiprocessing works
            pin_memory=True,
            drop_last=True,
        )

        start = time.time()

        # Iterate over dataset.
        for img in data_loader:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("Distributed Data Loader Sequential Throughput (imgs/s):",
              len(dataset) / (time.time() - start))

        # === Distributed Data Loader Shuffler

        # This will generate a random sampler, which will make the use
        # of batching wasteful
        sampler     = torch.utils.data.DistributedSampler(
            dataset, shuffle=True)

        data_loader = torch.utils.data.DataLoader(
            dataset,
            sampler=sampler,
            batch_size=10,          # pick random values here to test
            num_workers=4,          # num_workers > 1 to test multiprocessing works
            pin_memory=True,
            drop_last=True,
        )

        start = time.time()

        # Iterate over dataset.
        for img in data_loader:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("Distributed Data Loader Shuffle Throughput (imgs/s):",
              len(dataset) / (time.time() - start))
