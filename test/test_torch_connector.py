import time
import os
import logging
from typing import Union

import torch
import torch.distributed as dist
from aperturedb import Images
from aperturedb import PyTorchDataset
from torch.utils.data.dataloader import DataLoader
from torch.utils.data.dataset import Dataset

from aperturedb.ConnectorRest import ConnectorRest

logger = logging.getLogger(__name__)


class TestTorchDatasets():
    def validate_dataset(self, dataset: Union[DataLoader, Dataset], expected_length):
        start = time.time()

        count = 0
        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                logger.error("Empty image?")
                assert True == False
            count += len(img[1]) if isinstance(dataset, DataLoader) else 1
        assert count == expected_length

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {len(dataset) / time_taken}")

    def test_nativeContraints(self, db, utils, images):
        assert len(images) > 0
        # This is a hack against a bug in batch API.
        dim = 224 if isinstance(db, ConnectorRest) else 225
        query = [{
            "FindImage": {
                "constraints": {
                    "age": [">=", 0]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": dim,
                        "height": dim
                    }
                ],
                "results": {
                    "list": ["license"]
                }
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(
            db, query, label_prop="license")

        self.validate_dataset(dataset, utils.count_images())

    def test_datasetWithMultiprocessing(self, db, utils, images):
        len_limit = utils.count_images()
        # This is a hack against a bug in batch API.
        # TODO Fixme
        dim = 224 if isinstance(db, ConnectorRest) else 225
        query = [{
            "FindImage": {
                "constraints": {
                    "age": [">=", 0]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": dim,
                        "height": dim
                    }
                ],
                "results": {
                    "list": ["license"],
                    "limit": len_limit
                }
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(
            db, query, label_prop="license")

        self.validate_dataset(dataset, len_limit)

        # Distributed Data Loader Setup

        # Needed for init_process_group
        os.environ['MASTER_ADDR'] = 'localhost'
        os.environ['MASTER_PORT'] = '12355'

        dist.init_process_group("gloo", rank=0, world_size=1)

        # === Distributed Data Loader Sequential
        batch_size = 10
        data_loader = DataLoader(
            dataset,
            batch_size=batch_size,          # pick random values here to test
            num_workers=4,          # num_workers > 1 to test multiprocessing works
            pin_memory=True,
            drop_last=True,
        )

        self.validate_dataset(data_loader, len_limit)
        # === Distributed Data Loader Shuffler

        # This will generate a random sampler, which will make the use
        # of batching wasteful
        sampler     = torch.utils.data.DistributedSampler(
            dataset, shuffle=True)

        data_loader = DataLoader(
            dataset,
            sampler=sampler,
            batch_size=batch_size,          # pick random values here to test
            num_workers=4,          # num_workers > 1 to test multiprocessing works
            pin_memory=True,
            drop_last=True,
        )

        self.validate_dataset(data_loader, len_limit)
        dist.destroy_process_group()
