import time
import logging

import tensorflow as tf
from aperturedb.TensorFlowDataset import ApertureDBTensorFlowDataset

logger = logging.getLogger(__name__)


class TestTfDatasets():
    def validate_dataset(self, dataset: tf.data.Dataset, expected_length):
        start = time.time()

        count = 0
        # Iterate over dataset.
        for data, label in dataset:
            if data.dtype == tf.string:
                size = tf.strings.length(data).numpy()
            else:
                size = tf.size(data).numpy()
            if size == 0:
                logger.error("Empty data?")
                assert False
            count += 1
        assert count == expected_length

        time_taken = time.time() - start
        if time_taken != 0:
            logger.info(f"Throughput (imgs/s): {expected_length / time_taken}")

    def test_nativeConstraints(self, db, dataset_image_count):
        query = [{
            "FindImage": {
                "with_class": "DatasetImage",
                "constraints": ["$age", ">=", 0],
                "operations": [
                    {
                        "type": "resize",
                        "width": 16,
                        "height": 16
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

        self.validate_dataset(dataset, dataset_image_count)

    def test_batchedDataset(self, db, dataset_image_count):
        len_limit = dataset_image_count
        query = [{
            "FindImage": {
                "with_class": "DatasetImage",
                "constraints": ["$age", ">=", 0],
                "operations": [
                    {
                        "type": "resize",
                        "width": 16,
                        "height": 16
                    }
                ],
                "limit": len_limit,
                "results": {
                    "list": ["license"]
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
        for data, labels in batched_dataset:
            count += tf.shape(data)[0].numpy()
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

    def test_findBlob(self, db, dataset_blob_labels):
        query = [{
            "FindBlob": {
                "with_class": "DatasetBlob"
            }
        }]

        dataset_wrapper = ApertureDBTensorFlowDataset(
            db, query, label_prop="license")
        dataset = dataset_wrapper.get_dataset()

        actual = {}
        for blob, label in dataset:
            assert label.dtype == tf.int32
            actual[blob.numpy()] = label.numpy()
        assert actual == dataset_blob_labels

    def test_findVideo_mocked(self):
        from unittest.mock import patch
        import tensorflow as tf

        class DummyClient:
            def clone(self):
                return self

            def get_last_response_str(self):
                return ""

        query = [{"FindVideo": {"results": {"list": ["prop"]}}}]

        with patch('aperturedb.TensorFlowDataset.execute_query') as mock_exec:
            def side_effect(*args, **kwargs):
                batch_dict = {"total_elements": 1}
                entities = [{"prop": 1}]
                r = [{"FindVideo": {"batch": batch_dict, "entities": entities}}]
                b = [b"mock_video_bytes"]
                return None, r, b

            mock_exec.side_effect = side_effect
            dataset_wrapper = ApertureDBTensorFlowDataset(
                DummyClient(), query, label_prop="prop")
            dataset = dataset_wrapper.get_dataset()

            assert dataset.element_spec[1].dtype == tf.int32
            assert dataset.element_spec[0].dtype == tf.string

            count = 0
            for data, label in dataset:
                assert isinstance(data.numpy(), bytes)
                assert data.numpy() == b"mock_video_bytes"
                assert label.numpy() == 1
                count += 1
            assert count == 1
