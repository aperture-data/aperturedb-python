"""Offline command generation must match the negotiated server API."""
import json
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import pandas as pd

from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.DataModels import DescriptorDataModel, DescriptorSetDataModel, ImageDataModel
from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.PolygonDataCSV import PolygonDataCSV
from aperturedb.Query import ObjectType, Query, QueryBuilder, generate_add_query
from aperturedb.VideoDataCSV import VideoDataCSV


class Client:
    config = 'offline command fixture'

    def __init__(self, version=2):
        self.hello_version = version
        self.queries = []

    def clone(self):
        return self

    def query(self, query, blobs=None):
        self.queries.append(query)
        if 'GetIndexes' in query[0]:
            return [{'GetIndexes': {'status': 0, 'indexes': [{'kind': 'hnsw', 'params': {
                'metric': ['CS']}}]}}], []
        return [{'GetSchema': {'status': 0}}], []


class VersionedGenerators(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.blob = self.root / 'image.png'
        self.blob.write_bytes(b'\x89PNG\x00fixture')

    def data(self, cls, columns, row, **options):
        return cls(str(self.root / 'input.csv'), df=pd.DataFrame([row], columns=columns), **options)

    def test_typed_selectors_preserve_type_and_application_class(self):
        for target, command in [('entity', 'Entity'), ('image', 'Image'), ('descriptor', 'Descriptor'),
                                ('bounding_box', 'BoundingBox'), ('connection', 'Connection')]:
            self.assertEqual(QueryBuilder.find_command(target + ':Assets', {}),
                             {'Find' + command: {'with_class': 'Assets'}})
            self.assertEqual(QueryBuilder.add_command(target + ':Assets', {}),
                             {'Add' + command: {'class': 'Assets'}})

    def test_query_scope_is_preserved_for_builtin_object_types(self):
        modern = Query.spec(with_class='Photos', blobs=True)
        modern.db_object = ObjectType.IMAGE
        query, _ = modern.query(client=Client(2))
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')
        self.assertEqual(modern.with_class, 'Photos')
        self.assertEqual(modern.registry_type, '_Image')
        legacy = Query.spec(with_class='Photos')
        legacy.db_object = ObjectType.IMAGE
        query, _ = legacy.query(client=Client(1))
        self.assertNotIn('with_class', query[0]['FindImage'])
        self.assertEqual(legacy.with_class, '_Image')

    def test_specialized_queries_normalize_typed_class_selectors(self):
        image = Query.spec(with_class='image:Photos')
        image.db_object = ObjectType.IMAGE
        query, _ = image.query(hello_version=2)
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')
        self.assertEqual(image.with_class, 'Photos')
        descriptor = Query.spec(with_class='descriptor:Faces', vector=[
                                1.0, 2.0], k_neighbors=2)
        descriptor.db_object = ObjectType.DESCRIPTOR
        query, _ = descriptor.query(client=Client(2))
        self.assertEqual(query[0]['FindDescriptor']['with_class'], 'Faces')
        invalid = Query.spec(with_class='descriptor:Faces')
        invalid.db_object = ObjectType.IMAGE
        with self.assertRaisesRegex(ValueError, 'object type'):
            invalid.query(hello_version=2)
        mismatch = Query.spec(with_class='descriptor:Other', set='Faces')
        mismatch.db_object = ObjectType.DESCRIPTOR
        with self.assertRaisesRegex(ValueError, 'different descriptor classes'):
            mismatch.query(hello_version=2)

    def test_update_and_delete_builders_use_read_scope_keys(self):
        for operation in ['Find', 'Update', 'Delete']:
            self.assertEqual(QueryBuilder.build_command('image:Photos', {}, operation),
                             {operation + 'Image': {'with_class': 'Photos'}})
            self.assertEqual(QueryBuilder.build_command('People', {}, operation),
                             {operation + 'Entity': {'with_class': 'People'}})

    def test_connection_builder_supplies_the_class_once(self):
        params = {"src": 1, "dst": 2}
        for _ in range(2):
            self.assertEqual(QueryBuilder.add_command("connection:Owns", params),
                             {"AddConnection": {"class": "Owns", "src": 1, "dst": 2}})
        self.assertEqual(params, {"src": 1, "dst": 2})
        for duplicate in ({"class": {"connection": "Other"}}, {"with_class": "Owns"}):
            with self.assertRaisesRegex(ValueError, "already supplies"):
                QueryBuilder.find_command("connection:Owns", duplicate)
        for qualifier in ({"class": {"src": "People"}}, {"target": {"src": "entity"}}):
            with self.assertRaisesRegex(ValueError, "references"):
                QueryBuilder.add_command(
                    "connection:Owns", {**params, **qualifier})

    def test_connection_builder_groups_independent_endpoint_filters(self):
        endpoints = {"src": "People", "dst": "Photos"}
        triple = {**endpoints, "connection": "Owns"}
        targets = {"src": "entity", "dst": "image"}
        params = {"target": targets, "class": endpoints}
        for operation in ("Find", "Update", "Delete"):
            expected = {operation +
                        "Connection": {"class": triple, "target": targets}}
            from_name = QueryBuilder.build_command(
                "connection:Owns", params, operation)
            from_triple = QueryBuilder.build_command(ObjectType.CONNECTION,
                                                     {"class": triple, "target": targets}, operation)
            self.assertEqual(from_name, expected)
            self.assertEqual(from_triple, expected)
        self.assertEqual(params["class"], {"src": "People", "dst": "Photos"})
        for reference, other in (("src", "dst"), ("dst", "src")):
            body = {reference: 1, "class": {other: endpoints[other]},
                    "target": {other: targets[other]}, "results": {"count": True}}
            query = QueryBuilder.find_command("connection:Owns", body)
            grouped = {other: endpoints[other], "connection": "Owns"}
            self.assertEqual(query, {"FindConnection": {
                             **body, "class": grouped}})
            self.assertEqual(body["class"], {other: endpoints[other]})
            for duplicate in ({"class": {reference: endpoints[reference]}},
                              {"target": {reference: targets[reference]}}):
                with self.assertRaisesRegex(ValueError, reference + " reference"):
                    QueryBuilder.find_command(
                        "connection:Owns", {**body, **duplicate})

    def test_typed_selector_query_and_native_descriptor_search(self):
        query, _ = Query.spec(with_class='image:Photos').query(hello_version=2)
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')
        spec = Query.spec(set='Faces', vector=[1.0, 2.0], k_neighbors=3)
        spec.db_object = ObjectType.DESCRIPTOR
        query, blobs = spec.query(client=Client(2))
        self.assertNotIn('set', query[0]['FindDescriptor'])
        self.assertEqual(query[0]['FindDescriptor']['with_class'], 'Faces')
        self.assertEqual(query[0]['FindDescriptor']['engine'], 'HNSW')
        self.assertEqual(query[0]['FindDescriptor']['metric'], 'CS')
        self.assertEqual(blobs, [struct.pack('2f', 1.0, 2.0)])

    def test_media_csv_requires_explicit_class_only_for_v2(self):
        for cls, command, options in [(BlobDataCSV, 'AddBlob', {}),
                                      (ImageDataCSV, 'AddImage',
                                       {'check_image': False}),
                                      (VideoDataCSV, 'AddVideo', {'check_video': False})]:
            with self.subTest(loader=cls.__name__):
                data = self.data(cls, ['filename', 'id', 'constraint_id'], [
                                 str(self.blob), 7, 7], **options)
                query, _ = data[0]
                self.assertNotIn('class', query[0][command])
                data.set_hello_version(2)
                with self.assertRaisesRegex(ValueError, 'class_name'):
                    data.get_indices()
                data.class_name = 'Assets'
                query, blobs = data[0]
                self.assertEqual(query[0][command]['class'], 'Assets')
                self.assertEqual(blobs[0], self.blob.read_bytes())
                self.assertIn(command[3:].lower() +
                              ':Assets', data.get_indices()['entity'])

    def test_roi_csv_preserves_class_and_owner_selection(self):
        boxes = self.data(BBoxDataCSV, ['image_id', 'x_pos', 'y_pos', 'width', 'height'],
                          [7, 1, 2, 3, 4], hello_version=2, class_name='Boxes', image_class='Photos')
        query, _ = boxes[0]
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')
        self.assertEqual(query[1]['AddBoundingBox']['class'], 'Boxes')
        polygons = self.data(PolygonDataCSV, ['image_id', 'polygons'],
                             [7, json.dumps([[[0, 0], [1, 0], [1, 1]]])],
                             hello_version=2, class_name='Regions', image_class='Photos')
        query, _ = polygons[0]
        self.assertEqual(query[1]['AddPolygon']['class'], 'Regions')
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')

    def test_descriptor_csv_uses_row_set_as_native_class(self):
        filename = self.root / 'vectors.npy'
        np.save(filename, np.array([[1.0, 2.0]], dtype=np.float32))
        data = self.data(DescriptorDataCSV, ['filename', 'index', 'set', 'label', 'constraint_id'],
                         [str(filename), 0, 'Faces', 'alice', 7], hello_version=2)
        query, blobs = data[0]
        self.assertEqual(query[0]['AddDescriptor']['class'], 'Faces')
        self.assertNotIn('set', query[0]['AddDescriptor'])
        self.assertEqual(query[0]['AddDescriptor']['label'], 'alice')
        self.assertEqual(len(blobs[0]), 8)
        self.assertIn('descriptor:Faces', data.get_indices()['entity'])
        data.set_hello_version(1)
        self.assertEqual(data[0][0][0]['AddDescriptor']['set'], 'Faces')

    def test_descriptor_set_csv_creates_native_vector_index_and_preserves_metadata(self):
        data = self.data(DescriptorSetDataCSV, ['name', 'dimensions', 'engine', 'metric', 'model'],
                         ['Faces', 128, 'HNSW', 'CS', 'face-v1'], hello_version=2)
        query, blobs = data[0]
        index = query[0]['CreateIndex']
        self.assertEqual(index['class'], 'Faces')
        self.assertEqual(index['params']['dimensions'], 128)
        self.assertEqual(index['kind'], 'hnsw')
        self.assertEqual(index['metadata'], {'model': 'face-v1'})
        self.assertNotIn('_ref', index)
        self.assertFalse(blobs)
        self.assertEqual(data.get_indices(), {})
        data.set_hello_version(1)
        self.assertEqual(data[0][0][0]['AddDescriptorSet']['name'], 'Faces')
        conditional = self.data(DescriptorSetDataCSV, ['name', 'dimensions', 'engine', 'metric', 'constraint_id'],
                                ['Faces', 128, 'HNSW', 'CS', 7], hello_version=2)
        with self.assertRaisesRegex(ValueError, 'constraints'):
            conditional[0]

    def test_connection_csv_qualifies_both_native_index_endpoints(self):
        data = self.data(ConnectionDataCSV,
                         ['ConnectionClass', 'image:Photos@id',
                             'descriptor:Faces@id', 'constraint_id'],
                         ['embeds', 1, 2, 3], hello_version=2)
        query, _ = data[0]
        self.assertEqual(query[0]['FindImage']['with_class'], 'Photos')
        self.assertEqual(query[1]['FindDescriptor']['with_class'], 'Faces')
        self.assertEqual(query[2]['AddConnection'], {
            'class': 'embeds', 'src': 1, 'dst': 2, 'if_not_found': ['$id', '==', 3]})
        index = data.get_indices()['commands'][0]['CreateIndex']
        self.assertEqual(index, {
            'target': {'src': 'image', 'dst': 'descriptor'},
            'class': {'src': 'Photos', 'connection': 'embeds', 'dst': 'Faces'},
            'property': 'id', 'kind': 'ordered'})
        legacy = self.data(ConnectionDataCSV, [
                           'ConnectionClass', '_Image@id', '_Descriptor@id'], ['embeds', 1, 2])
        query, _ = legacy[0]
        self.assertEqual(next(iter(query[0])), 'FindImage')
        self.assertEqual(query[2]['AddConnection'], {
                         'class': 'embeds', 'src': 1, 'dst': 2})
        legacy.set_hello_version(2)
        with self.assertRaisesRegex(ValueError, 'explicit typed classes'):
            legacy[0]

    def test_parallel_query_binds_version_before_generator_setup(self):
        class Probe(ParallelQuery):
            def query_setup(inner, generator):
                self.assertEqual(generator.hello_version, 2)
                self.assertIn('blob:Files', generator.get_indices()['entity'])

            def batched_run(inner, generator, *args):
                inner.sample = generator[0]

        generator = self.data(BlobDataCSV, ['filename', 'constraint_id'], [
                              str(self.blob), 1], class_name='Files')
        loader = Probe(Client(2), dry_run=True)
        loader.query(generator, numthreads=1)
        self.assertEqual(loader.sample[0][0]['AddBlob']['class'], 'Files')

    def test_csv_slices_rebind_without_rewriting_raw_query_generators(self):
        source = self.data(BlobDataCSV, ['filename', 'constraint_id'], [
                           str(self.blob), 1], class_name='Files')
        sampled = source[:1][:1]
        self.assertNotIn('class', sampled[0][0][0]['AddBlob'])
        sampled.set_hello_version(2)
        self.assertEqual(sampled[0][0][0]['AddBlob']['class'], 'Files')
        self.assertIn('blob:Files', sampled.get_indices()['entity'])
        from aperturedb.Subscriptable import Subscriptable

        class Raw(Subscriptable):
            def __len__(self):
                return 1

            def getitem(self, index):
                return [{'AddImage': {}}], [b'raw']

        raw = Raw()[:1]
        raw.set_hello_version(2)
        self.assertEqual(raw[0], ([{'AddImage': {}}], [b'raw']))

    def test_dask_partitions_preserve_class_version_and_validation_options(self):
        from aperturedb.DaskManager import DaskManager
        parent = self
        generator = self.data(BlobDataCSV, ['filename', 'id'], [str(self.blob), 1],
                              class_name='Files', hello_version=2, strict_response_validation=True)
        frame = pd.DataFrame({'filename': [str(self.blob)] * 3, 'id': [1, 2, 3]},
                             index=pd.RangeIndex(40, 43))

        class Frame:
            def map_partitions(inner, function, *args):
                inner.result = [function(frame, *args)]
                return inner

            def persist(inner):
                return inner

            def compute(inner):
                return inner.result

        class WorkerClient(Client):
            def __init__(inner, config=None):
                super().__init__(2)
                inner.config = config

            def close(inner):
                pass

        class WorkerLoader:
            def __init__(inner, client, dry_run=False):
                inner.times_arr = []
                inner.error_counter = 0

            def query(inner, generator, **kwargs):
                parent.assertEqual(generator.class_name, 'Files')
                parent.assertEqual(generator.hello_version, 2)
                parent.assertTrue(generator.strict_response_validation)
                parent.assertEqual(
                    generator[0][0][0]['AddBlob']['class'], 'Files')

            def get_succeeded_queries(inner):
                return 1

            def get_succeeded_commands(inner):
                return 1

        generator.df = Frame()
        manager = object.__new__(DaskManager)
        manager._client = SimpleNamespace(close=lambda: None)
        manager._cluster = SimpleNamespace(close=lambda: None)
        results, _ = manager.run(
            WorkerLoader, WorkerClient(), generator, 2, False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].succeeded_queries, 2)

    def test_embedding_transformers_choose_class_or_set_without_rewriting_image_commands(self):
        import copy
        import importlib
        from types import ModuleType
        from unittest.mock import patch
        payload = struct.pack('2f', 1.0, 2.0)

        class Data:
            def __len__(inner):
                return 1

            def __getitem__(inner, index):
                return copy.deepcopy(([{'AddImage': {'_ref': 1, 'class': 'Photos'}}], [b'image']))

        for package, module, name in [('clip', 'clip_pytorch_embeddings', 'CLIPPyTorchEmbeddings'),
                                      ('facenet', 'facenet_pytorch_embeddings', 'FacenetPyTorchEmbeddings')]:
            dependency = ModuleType('aperturedb.transformers.' + package)
            dependency.generate_embedding = lambda *_: payload
            dependency.descriptor_set = 'Faces'
            with patch.dict('sys.modules', {dependency.__name__: dependency}):
                target = importlib.import_module(
                    'aperturedb.transformers.' + module)
                with patch.object(target, "generate_embedding", dependency.generate_embedding):
                    for version in [1, 2]:
                        transformer = getattr(target, name)(
                            Data(), search_set_name='Faces')
                        transformer.get_utils = lambda: SimpleNamespace(
                            client=Client(version), add_descriptorset=lambda *a, **k: True)
                        if package == 'facenet':
                            transformer._get_embedding_from_blob = lambda *_: payload
                        commands, blobs = transformer[0]
                        descriptor = commands[-1]['AddDescriptor']
                        self.assertEqual(
                            descriptor['class' if version == 2 else 'set'], 'Faces')
                        self.assertEqual(
                            commands[0]['AddImage']['class'], 'Photos')
                        self.assertEqual(blobs[-1], payload)
                        self.assertEqual(descriptor['connect'], {'ref': 1,
                                                                 **({'class': 'has_descriptor'} if version == 2 else {})})

    def test_existing_indexes_keep_native_connection_categories_distinct(self):
        from aperturedb.ParallelLoader import ParallelLoader
        loader = object.__new__(ParallelLoader)
        loader.utils = SimpleNamespace(get_indexes=lambda **kwargs: [
            {'target': {'src': 'entity', 'dst': 'image'},
             'class': {'src': 'People', 'connection': 'owns', 'dst': name}, 'property': 'id'}
            for name in ['Photos', 'Diagrams']])
        indexes = loader.get_existing_indices()['connection']
        self.assertEqual(len(indexes), 2)
        self.assertEqual(
            indexes[('owns', 'entity', 'People', 'image', 'Photos')], {'id'})

    @unittest.skipUnless(__import__('os').environ.get('SDK_LIVE_NATIVE_PORTS'), 'Set SDK_LIVE_NATIVE_PORTS for disposable live databases')
    def test_live_csv_indexing_and_ingestion_on_both_server_apis(self):
        import os
        from uuid import uuid4
        from aperturedb.Connector import Connector
        from aperturedb.EntityDataCSV import EntityDataCSV
        from aperturedb.ParallelLoader import ParallelLoader
        for port in os.environ['SDK_LIVE_NATIVE_PORTS'].split(','):
            with self.subTest(port=port):
                client = Connector(host='127.0.0.1', port=int(
                    port), user='admin', password='admin', use_ssl=False)
                prefix = 'SDKCSV' + uuid4().hex
                prop = 'csv_' + uuid4().hex[:12]
                people, files, edge = prefix + 'People', prefix + 'Files', prefix + 'Owns'
                try:
                    version = client.hello_version
                    loader = ParallelLoader(client)
                    entities = self.data(EntityDataCSV, [
                                         'EntityClass', prop, 'constraint_' + prop], [people, 'person', 'person'])
                    loader.ingest(entities, numthreads=1, stats=False)
                    self.assertEqual(loader.error_counter, 0)
                    blobs = self.data(BlobDataCSV, ['filename', prop, 'constraint_' + prop], [
                                      str(self.blob), 'file', 'file'], class_name=files)
                    loader.ingest(blobs, numthreads=1, stats=False)
                    self.assertEqual(loader.error_counter, 0)
                    columns = ['ConnectionClass', ('entity:' + people if version >= 2 else people) + '@' + prop,
                               ('blob:' + files if version >= 2 else '_Blob') + '@' + prop, prop, 'constraint_' + prop]
                    connections = self.data(ConnectionDataCSV, columns, [
                                            edge, 'person', 'file', 'edge', 'edge'])
                    loader.ingest(connections, numthreads=1, stats=False)
                    self.assertEqual(loader.error_counter, 0)
                    answer, _ = client.query(
                        [{'FindConnection': {'with_class': edge, 'results': {'count': True}}}])
                    self.assertEqual(answer[0]['FindConnection']['count'], 1)
                    if version >= 2:
                        self.assertIn((edge, 'entity', people, 'blob', files),
                                      loader.get_existing_indices()['connection'])
                        utils = loader.utils
                        classes = {'src': people,
                                   'connection': edge, 'dst': files}
                        target = {'src': 'entity', 'dst': 'blob'}
                        indexes = utils.get_indexes(
                            target, classes, prop)
                        self.assertEqual([index['class']
                                         for index in indexes], [classes])
                        self.assertIn(edge, utils.visualize_schema().source)
                        self.assertTrue(utils.remove_connection_index(
                            classes, prop, target=target))
                        self.assertTrue(utils.create_connection_index(
                            classes, prop, target=target))
                finally:
                    client.query([{'DeleteConnection': {'with_class': edge}},
                                  {'DeleteEntity': {'with_class': people}},
                                  {'DeleteBlob': {**({'with_class': files} if client.hello_version >= 2 else {}),
                                                  'constraints': ['$' + prop, '==', 'file']}}])
                    client.close()

    def test_model_queries_use_model_class_and_native_descriptor_scope(self):
        class Photo(ImageDataModel):
            class_name = 'Photos'

        photo = Photo(url=str(self.blob))
        query, blobs, _ = generate_add_query(photo, hello_version=2)
        self.assertEqual(query[0]['AddImage']['class'], 'Photos')
        self.assertEqual(blobs, [self.blob.read_bytes()])
        cached, blobs, _ = generate_add_query(
            photo, cached=[photo.id], hello_version=2)
        self.assertEqual(cached[0]['FindImage']['with_class'], 'Photos')
        self.assertFalse(blobs)
        legacy, _, _ = generate_add_query(photo, hello_version=1)
        self.assertNotIn('class', legacy[0]['AddImage'])
        definition = DescriptorSetDataModel(name='Faces', dimensions=2)
        query, _, _ = generate_add_query(definition, hello_version=2)
        self.assertEqual(query[0]['CreateIndex']['class'], 'Faces')
        self.assertNotIn('_ref', query[0]['CreateIndex'])
        with self.assertRaisesRegex(ValueError, 'graph objects'):
            generate_add_query(definition, hello_version=2, parent=1)
        descriptor = DescriptorDataModel(vector=[1.0, 2.0], set=definition)
        query, _, _ = generate_add_query(descriptor, hello_version=2)
        self.assertEqual(query[0]['AddDescriptor']['class'], 'Faces')


if __name__ == '__main__':
    unittest.main()
