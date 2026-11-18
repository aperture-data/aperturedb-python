from aperturedb.ServerCompatibility import hello_version as negotiated_hello_version
import tensorflow as tf
from aperturedb.TensorFlowData import TensorFlowData
from typing import List, Tuple
from aperturedb.Images import np_arr_img_to_bytes


class Cifar10DataTensorFlow(TensorFlowData):
    """
    **ApertureDB ingestable Dataset, which is sourced from
    [Cifar10 (tensorflow.datasets)](https://www.tensorflow.org/datasets/catalog/cifar10)**
    """

    def __init__(self, client=None, hello_version=None):
        self.configure_server(client, hello_version)
        (x_train, y_train), (x_test, y_test) = tf.keras.datasets.cifar10.load_data()
        self.x = tf.concat([x_train, x_test], axis=0)
        self.y = tf.concat([tf.squeeze(y_train), tf.squeeze(y_test)], axis=0)
        self.train_len = x_train.shape[0]

    def __len__(self):
        return self.x.shape[0]

    def configure_server(self, client=None, hello_version=None):
        """Configure the generated commands for the connected server."""
        version = hello_version
        if version is None:
            version = client if type(
                client) is int else negotiated_hello_version(client)
        if type(version) is not int or version not in (1, 2):
            raise ValueError("hello_version must be 1 or 2")
        self.hello_version = version
        return self

    def set_hello_version(self, version):
        # ParallelLoader calls this before generating or resampling commands.
        return self.configure_server(hello_version=version)

    def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
        x, y = self.x[idx], self.y[idx]
        q = [{
            "AddImage": {
                "_ref": 1
            }
        }]
        q[0]["AddImage"]["properties"] = {
            "label": str(y.numpy()),
            "train": True if idx < self.train_len else False
        }

        if self.hello_version >= 2:
            q[0]["AddImage"]["class"] = "Cifar10Image"
        return q, [np_arr_img_to_bytes(x.numpy())]
