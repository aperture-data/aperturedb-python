"""Exercise published generator examples without importing/downloading ML data."""
import ast
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import List, Tuple
import unittest
from unittest.mock import patch

from aperturedb.ServerCompatibility import hello_version

EXAMPLES = Path(__file__).resolve().parents[1] / 'examples'


def definitions(name, wanted, namespace):
    source = ast.parse((EXAMPLES / name).read_text())
    tree = ast.Module(body=[node for node in source.body
                            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in wanted],
                      type_ignores=[])
    exec(compile(tree, str(EXAMPLES / name), 'exec'), namespace)
    return namespace


def generator(name, class_name):
    namespace = dict(negotiated_hello_version=hello_version, List=List, Tuple=Tuple,
                     PyTorchData=object, TensorFlowData=object, KaggleData=object,
                     pd=SimpleNamespace(DataFrame=object),
                     image_to_bytes=lambda _: b'image', np_arr_img_to_bytes=lambda _: b'image',
                     polygonFromMask=lambda _: [[0, 0, 3, 0, 3, 4, 0, 4]])
    import os
    namespace['os'] = os
    cls = definitions(name, {class_name}, namespace)[class_name]
    return cls.__new__(cls)


class ExampleCompatibilityTests(unittest.TestCase):
    def check_classes(self, data, expected):
        for version in (1, 2):
            data.configure_server(SimpleNamespace(hello_version=version))
            query, blobs = data.generate_query(0)
            self.assertEqual(blobs, [b'image'])
            seen = set()
            for command in query:
                name, parameters = next(iter(command.items()))
                if name in expected:
                    seen.add(name)
                    if version == 2:
                        self.assertEqual(parameters['class'], expected[name])
                    else:
                        self.assertNotIn('class', parameters)
            self.assertEqual(seen, set(expected))
        data.set_hello_version(2)
        self.assertEqual(data.hello_version, 2)
        with self.assertRaises(ValueError):
            data.configure_server(hello_version=3)

    def test_coco_image_box_polygon_classes(self):
        data = generator('CocoDataPyTorch.py', 'CocoDataPyTorch')
        data.dataset_name = 'example'
        annotation = {'category_id': 1, 'bbox': [
            0, 0, 3, 4], 'segmentation': []}
        data.loaded_dataset = [(object(), [annotation])]
        data.coco_detection = SimpleNamespace(coco=SimpleNamespace(
            loadCats=lambda _: [{'supercategory': 'Animal', 'name': 'Bird'}], annToMask=lambda _: object()))
        self.check_classes(data, {'AddImage': 'CocoImage', 'AddBoundingBox': 'CocoBoundingBox',
                                  'AddPolygon': 'CocoPolygon'})

    def test_celeba_image_and_face_classes(self):
        data = generator('CelebADataKaggle.py', 'CelebADataKaggle')
        record = {name: 1 for name in ('x_1', 'y_1', 'width', 'height', 'lefteye_x', 'lefteye_y',
                  'righteye_x', 'righteye_y', 'nose_x', 'nose_y', 'leftmouth_x', 'leftmouth_y',
                                       'rightmouth_x', 'rightmouth_y')}
        record['image_id'] = 'image.png'
        data.collection = [record]
        with tempfile.TemporaryDirectory() as root:
            data.workdir = root
            path = Path(root) / 'img_align_celeba/img_align_celeba/image.png'
            path.parent.mkdir(parents=True)
            path.write_bytes(b'image')
            self.check_classes(
                data, {'AddImage': 'CelebAImage', 'AddBoundingBox': 'CelebAFace'})

    def test_cifar_image_class(self):
        data = generator('Cifar10DataTensorFlow.py', 'Cifar10DataTensorFlow')
        data.x = [SimpleNamespace(numpy=lambda: object())]
        data.y = [SimpleNamespace(numpy=lambda: 1)]
        data.train_len = 1
        self.check_classes(data, {'AddImage': 'Cifar10Image'})

    def test_model_example_binds_query_generation_to_client(self):
        tree = ast.parse(
            (EXAMPLES / 'loading_with_models/add_video_model.py').read_text())
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == 'generate_add_query']
        self.assertEqual(len(calls), 2)
        self.assertTrue(
            all(any(keyword.arg == 'client' for keyword in call.keywords) for call in calls))

    def test_http_example_uses_typed_classes_and_publishes_only_complete_downloads(self):
        namespace = definitions('rest_api/rest_api.py', {'add_image', 'download_image'},
                                dict(Path=Path, tempfile=tempfile, json=json))

        class Client:
            def __init__(self, version, fail=False):
                self.hello_version, self.fail, self.calls = version, fail, []

            def check_status(self, _):
                return 0

            def query(self, commands, blobs):
                self.calls.append((commands, blobs))
                return [{'AddImage': {'status': 0}}], []

            @contextmanager
            def query_stream(self, commands):
                self.calls.append((commands, []))

                def blobs():
                    yield SimpleNamespace(iter_chunks=lambda: iter([b'raw', b'image']))
                    if self.fail:
                        raise ConnectionError('Missing closing MIME boundary')
                yield SimpleNamespace(json=[{'FindImage': {'status': 0}}], blobs=blobs())

        for version in (1, 2):
            with tempfile.TemporaryDirectory() as root, patch('builtins.print'):
                root = Path(root)
                source = root / 'source.png'
                source.write_bytes(b'image')
                client = Client(version)
                namespace['add_image'](
                    client, source, 'id', 'RestExampleImage')
                add = client.calls[-1][0][0]['AddImage']
                self.assertEqual('class' in add, version == 2)
                paths = namespace['download_image'](
                    client, 'id', 'RestExampleImage', root / 'out')
                self.assertEqual(paths[0].read_bytes(), b'rawimage')
                failing = Client(version, fail=True)
                with self.assertRaises(ConnectionError):
                    namespace['download_image'](
                        failing, 'id', 'RestExampleImage', root / 'failed')
                self.assertEqual(list((root / 'failed').iterdir()), [])
