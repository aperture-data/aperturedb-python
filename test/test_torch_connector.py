import time
import os
import logging
from typing import Union

import torch
import torch.distributed as dist
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
        for data in dataset:
            if len(data[0]) == 0:
                logger.error("Empty data?")
                assert False
            count += len(data[1]) if isinstance(dataset, DataLoader) else 1
        assert count == expected_length

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {len(dataset) / time_taken}")

    def test_nativeConstraints(self, db, utils, images):
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

    def test_findBlob(self, db, utils, insert_data_from_csv):
        blobs, _ = insert_data_from_csv("./input/blobs.adb.csv")
        assert len(blobs) > 0
        query = [{
            "FindBlob": {
                "results": {}
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(
            db, query, label_prop="license")

        assert len(dataset) == utils.count_entities("_Blob")
        for blob, label in dataset:
            # For FindBlob, the return is raw bytes and label should be an int when label_prop='license'
            assert isinstance(blob, bytes)
            assert isinstance(label, int)
            break

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

    def test_findVideo_mocked(self):
        from unittest.mock import patch

        class DummyClient:
            def clone(self):
                return self

            def get_last_response_str(self):
                return ""

        query = [{"FindVideo": {"results": {"list": ["prop"]}}}]

        with patch('aperturedb.PyTorchDataset.execute_query') as mock_exec:
            def execute_query_side_effect(*args, **kwargs):
                batch_dict = {"total_elements": 1}
                entities = [{"prop": 1}]
                r = [{"FindVideo": {"batch": batch_dict, "entities": entities}}]
                b = [b"mock_video_bytes"]
                return None, r, b

            mock_exec.side_effect = execute_query_side_effect
            dataset = PyTorchDataset.ApertureDBDataset(
                DummyClient(), query, label_prop="prop")

            assert len(dataset) == 1
            for blob, label in dataset:
                assert isinstance(blob, bytes)
                assert blob == b"mock_video_bytes"
                assert label == 1
                break

    def test_find_frame_mocked(self):
        from unittest.mock import patch
        import numpy as np
        import cv2

        class DummyClient:
            def clone(self):
                return self

            def get_last_response_str(self):
                return ""

        query = [{"FindFrame": {"results": {"list": ["prop"]}}}]

        with patch('aperturedb.PyTorchDataset.execute_query') as mock_exec:
            def execute_query_side_effect(*args, **kwargs):
                batch_dict = {"total_elements": 1}
                entities = [{"prop": 1}]
                r = [{"FindFrame": {"batch": batch_dict, "entities": entities}}]
                img = np.zeros((10, 10, 3), dtype=np.uint8)
                img[0, 0] = [255, 0, 0]  # BGR format for OpenCV
                is_success, buffer = cv2.imencode(".png", img)
                assert is_success, "Failed to encode image"
                b = [buffer.tobytes()]
                return None, r, b

            mock_exec.side_effect = execute_query_side_effect
            dataset = PyTorchDataset.ApertureDBDataset(
                DummyClient(), query, label_prop="prop")

            assert len(dataset) == 1
            for blob, label in dataset:
                assert isinstance(blob, np.ndarray)
                assert blob.shape == (10, 10, 3)
                assert np.array_equal(
                    blob[0, 0], [0, 0, 255]), "Expected RGB color conversion"
                assert isinstance(label, int)
                assert label == 1
                break
