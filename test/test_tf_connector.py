import time
import logging

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
            if tf.size(img).numpy() == 0:
                logger.error("Empty image?")
                assert False
            count += 1
        assert count == expected_length

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {expected_length / time_taken}")

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

        dataset_wrapper = ApertureDBTensorFlowDataset(
            db, query, label_prop="license")
        dataset = dataset_wrapper.get_dataset()

        self.validate_dataset(dataset, utils.count_images())

    def test_batchedDataset(self, db, utils, images):
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
            count += tf.shape(imgs)[0].numpy()
        assert count == len_limit

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {len_limit / time_taken}")

    def test_dynamic_label_dtype(self):
        from unittest.mock import patch
        import numpy as np
        import cv2

        class DummyClient:
            def clone(self):
                return self

            def get_last_response_str(self):
                return ""

        query = [{"FindImage": {"results": {"list": ["prop"]}}}]

        with patch('aperturedb.TensorFlowDataset.execute_query') as mock_exec:
            def side_effect_int(*args, **kwargs):
                batch_dict = {"total_elements": 1}
                entities = [{"prop": 42}]  # int
                r = [{"FindImage": {"batch": batch_dict, "entities": entities}}]
                img = np.zeros((10, 10, 3), dtype=np.uint8)
                _, b_img = cv2.imencode('.jpg', img)
                b = [b_img.tobytes()]
                return None, r, b

            mock_exec.side_effect = side_effect_int
            dataset_wrapper = ApertureDBTensorFlowDataset(
                DummyClient(), query, label_prop="prop")
            dataset = dataset_wrapper.get_dataset()
            assert dataset.element_spec[1].dtype == tf.int32

            def side_effect_float(*args, **kwargs):
                batch_dict = {"total_elements": 1}
                entities = [{"prop": 3.14}]  # float
                r = [{"FindImage": {"batch": batch_dict, "entities": entities}}]
                img = np.zeros((10, 10, 3), dtype=np.uint8)
                _, b_img = cv2.imencode('.jpg', img)
                b = [b_img.tobytes()]
                return None, r, b

            mock_exec.side_effect = side_effect_float
            dataset_wrapper_f = ApertureDBTensorFlowDataset(DummyClient(
            ), [{"FindImage": {"results": {"list": ["prop"]}}}], label_prop="prop")
            dataset_f = dataset_wrapper_f.get_dataset()
            assert dataset_f.element_spec[1].dtype == tf.float32

    def test_findBlob(self, db, utils, insert_data_from_csv):
        blobs, _ = insert_data_from_csv("./input/blobs.adb.csv")
        assert len(blobs) > 0
        query = [{
            "FindBlob": {
                "results": {}
            }
        }]

        dataset_wrapper = ApertureDBTensorFlowDataset(
            db, query)
        dataset = dataset_wrapper.get_dataset()

        count = 0
        for blob, label in dataset:
            assert isinstance(blob.numpy(), bytes)
            assert label.numpy().decode("utf-8") == "none"
            count += 1

        assert count == utils.count_entities("_Blob")
