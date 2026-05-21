import time
import os
import logging
from typing import Union

import tensorflow as tf
from aperturedb.TensorFlowDataset import ApertureDBTensorFlowDataset

from aperturedb.ConnectorRest import ConnectorRest

logger = logging.getLogger(__name__)

class TestTfDatasets():
    def validate_dataset(self, dataset: tf.data.Dataset, expected_length):
        start = time.time()

        count = 0
        # Iterate over dataset.
        for img, label in dataset:
            if tf.shape(img)[0] < 0:
                logger.error("Empty image?")
                assert True == False
            count += 1
        assert count == expected_length

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {expected_length / time_taken}")

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

        dataset_wrapper = ApertureDBTensorFlowDataset(
            db, query, label_prop="license")
        dataset = dataset_wrapper.get_dataset()

        self.validate_dataset(dataset, utils.count_images())

    def test_datasetWithMultiprocessing(self, db, utils, images):
        len_limit = utils.count_images()
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
                    "list": ["license"],
                    "limit": len_limit
                }
            }
        }]

        dataset_wrapper = ApertureDBTensorFlowDataset(
            db, query, label_prop="license")
        dataset = dataset_wrapper.get_dataset()
        # Test batched dataset
        batched_dataset = dataset.batch(10).prefetch(tf.data.AUTOTUNE)

        start = time.time()
        count = 0
        for imgs, labels in batched_dataset:
            count += imgs.shape[0]
        assert count == len_limit
