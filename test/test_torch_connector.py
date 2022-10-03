import time
import os
import logging

import torch
import torch.distributed as dist
from aperturedb import Images
from aperturedb import PyTorchDataset

logger = logging.getLogger(__name__)


class TestTorchDatasets():
    def validate_dataset(self, dataset):
        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                logger.error("Empty image?")
                assert True == False

        logger.info("\n")
        logger.info("Throughput (imgs/s):",
                    len(dataset) / (time.time() - start))

    def test_omConstraints(self, db, utils, images):
        assert len(images) > 0
        const = Images.Constraints()
        const.greaterequal("age", 0)
        dataset = PyTorchDataset.ApertureDBDatasetConstraints(
            db, constraints=const)

        assert len(dataset) == utils.count_images()
        self.validate_dataset(dataset)

    def test_nativeContraints(self, db, utils, images):
        assert len(images) > 0
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

        assert len(dataset) == utils.count_images()
        self.validate_dataset(dataset)

    def test_datasetWithMultiprocessing(self, db, utils):
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

        assert len(dataset) == utils.count_images()
        self.validate_dataset(dataset)
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

        self.validate_dataset(data_loader)
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

        self.validate_dataset(data_loader)
