import tensorflow as tf
from aperturedb.TensorflowData import TensorflowData
from typing import List, Tuple
from PIL import Image
import io


class Cifar100DataTensorflow(TensorflowData):
    def __init__(self):
        (x_train, y_train), (x_val, y_val) = tf.keras.datasets.cifar100.load_data()
        x = tf.concat([x_train, x_val], axis=0)
        y = tf.concat([tf.squeeze(y_train), tf.squeeze(y_val)], axis=0)
        ds = tf.data.Dataset.from_tensor_slices((x, y))
        super().__init__(ds)

    def __image__from__cifar100__(self, arr):
        bytes = io.BytesIO()
        Image.fromarray(arr, 'RGB').save(bytes, format='JPEG')
        return bytes.getvalue()

    def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
        x, y = self.loaded_dataset[idx]
        q = [{
            "AddImage": {
                "_ref": 1
            }
        }]
        q[0]["AddImage"]["properties"] = {
            "label": str(y.numpy()),
            "stage": "train" if idx < 50000 else "val"
        }

        return q, [self.__image__from__cifar100__(x.numpy())]
