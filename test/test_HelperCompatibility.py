"""Optional live semantic SDK compatibility matrix.

Uses ADB_COMPAT_ENDPOINTS and credentials documented by test_ProtocolCompatibility.
Each case creates unique classes and removes only its own data. No cloud services,
model downloads, or global remove_all_objects calls are used.
"""
import contextlib
import importlib.util
import io
import json
import os
import unittest
import uuid
from urllib.parse import quote

import numpy as np
from PIL import Image
from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb.Utils import Utils
from aperturedb.Descriptors import Descriptors
from aperturedb.Constraints import Constraints
from aperturedb.ServerCompatibility import schema_for_display
from aperturedb.SPARQL import SPARQL
from aperturedb.Images import Images


def endpoint_client(endpoint):
    cls = ConnectorRest if endpoint.get("rest") else Connector
    return cls(host=endpoint.get("host", "127.0.0.1"), port=endpoint["port"],
               user=os.getenv("ADB_COMPAT_USERNAME", "admin"),
               password=os.getenv("ADB_COMPAT_PASSWORD", "admin"),
               use_ssl=endpoint.get("use_ssl", False),
               ca_cert=endpoint.get("ca_cert"),
               verify_hostname=endpoint.get("verify_hostname", False))


def okay(response):
    assert isinstance(response, list), response
    assert all(next(iter(command.values())).get("status") in (0, 2)
               for command in response), response
    return response


def exercise_vectors(endpoint):
    cls = ConnectorRest if endpoint.get("rest") else Connector
    port, version = endpoint["port"], endpoint["hello_version"]
    results = []
    prefix = 'SdkAudit' + uuid.uuid4().hex[:10]
    dclass = prefix + 'Vectors'
    eclass = prefix + 'Owners'
    edge = prefix + 'Links'
    db = endpoint_client(endpoint)
    utils = Utils(db)
    created = False
    removed = False
    try:
        assert db.hello_version == version, (port, db.hello_version)
        created = utils.create_vector_index(dclass, 2, metric=['L2'], engines={
                                            'flat': {}}, metadata={'review_label': prefix})
        assert created
        assert utils.create_vector_index(dclass, 2, metric=['L2'], engines={
                                         'flat': {}}, metadata={'review_label': prefix})
        body = {'class' if version == 2 else 'set': dclass}
        q = [{'AddEntity': {'class': eclass, '_ref': 1, 'properties': {'owner_label': prefix}}},
             {'AddDescriptor': {**body, '_ref': 2,
                                'properties': {'review_rank': 0, 'review_name': 'near'}}},
             {'AddDescriptor': {**body, '_ref': 3,
                                'properties': {'review_rank': 1, 'review_name': 'far'}}},
             {'AddConnection': {'class': edge, 'src': 1, 'dst': 2, 'properties': {'link_rank': 1}}}]
        okay(db.query(q, [np.array([1., 0.], dtype=np.float32).tobytes(
        ), np.array([0., 1.], dtype=np.float32).tobytes()])[0])
        assert utils.count_descriptors_in_set(dclass) == 2
        assert dclass in utils.get_descriptorset_list()
        assert utils.count_descriptorsets() >= 1
        desc = Descriptors(db)
        desc.find_similar(dclass, [1., 0.], 2,
                          distances=True, blobs=True, metric='L2')
        assert len(desc.response) == 2, desc.response
        assert desc.response[0]['review_name'] == 'near', desc.response
        assert np.array_equal(desc.response[0]['vector'], np.array(
            [1., 0.], dtype=np.float32)), desc.response
        filtered = Constraints().equal('review_rank', 0)
        desc.find_similar(dclass, [1., 0.], 2,
                          constraints=filtered, metric='L2')
        assert len(desc.response) == 1, desc.response
        if version == 2:
            indexes = utils.get_vector_indexes(dclass)
            assert indexes[0]['metadata']['review_label'] == prefix, indexes
            assert utils.create_entity_index(
                'descriptor:' + dclass, 'review_rank')
            endpoints = {'src_type': 'entity', 'src_class': eclass,
                         'dst_type': 'descriptor', 'dst_class': dclass}
            assert utils.create_connection_index(
                edge, 'link_rank', **endpoints)
            selected = utils.get_indexes(target='connection', class_name=edge)
            assert selected and isinstance(
                selected[0]['target'], dict), selected
        else:
            dset = db.query([{'FindDescriptorSet': {'with_name': dclass, 'results': {
                            'all_properties': True}, 'metrics': True, 'engines': True, 'dimensions': True}}])[0]
            assert dset[0]['FindDescriptorSet']['entities'][0]['review_label'] == prefix, dset
        display = schema_for_display(utils.get_schema())
        nodekey = 'entity:' + eclass if version == 2 else eclass
        assert nodekey in display['entities']['classes'], display
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            utils.summary()
        assert eclass in out.getvalue(), out.getvalue()
        graph = utils.visualize_schema()
        assert eclass in graph.source, graph.source
        assert utils.remove_descriptorset(
            dclass), utils.client.get_last_response_str()
        removed = True
        assert dclass not in utils.get_descriptorset_list(), utils.get_descriptorset_list()
        remaining = db.query(
            [{'FindEntity': {'with_class': eclass, 'results': {'count': True}}}])[0]
        okay(remaining)
        assert remaining[0]['FindEntity']['count'] == 1, remaining
        if version == 2:
            schema = utils.get_schema()
            assert not any(n['type'] == 'descriptor' and n['class']
                           == dclass for n in schema['nodes']), schema
            assert not any(e['class'] == edge for e in schema['edges']), schema
            assert not any(i['class'] in {dclass, edge}
                           for i in utils.get_indexes()), utils.get_indexes()
        results.append({'transport': cls.__name__, 'port': port,
                       'version': version, 'result': 'PASS', 'prefix': prefix})
        print(json.dumps(results[-1]), flush=True)
    finally:
        try:
            if created and not removed:
                utils.remove_descriptorset(dclass)
            db.query([{'DeleteEntity': {'with_class': eclass}}])
            if version == 2:
                db.query(
                    [{'RemoveClass': {'target': 'entity', 'class': eclass}}])
        finally:
            utils.client.close()
            db.close()


def exercise_sparql(endpoint):
    cls = ConnectorRest if endpoint.get("rest") else Connector
    port, version = endpoint["port"], endpoint["hello_version"]
    results = []
    prefix = 'SparqlAudit' + uuid.uuid4().hex[:9]
    shared = prefix + 'Shared'
    vectors = prefix + 'Vectors'
    edge = prefix + 'Links'
    vedge = prefix + 'VectorLinks'
    db = endpoint_client(endpoint)
    utils = Utils(db)
    try:
        assert utils.create_vector_index(
            vectors, 2, metric=['L2'], engines={'flat': {}})
        output = io.BytesIO()
        Image.new('RGB', (2, 3), (11, 22, 33)).save(output, format='PNG')
        image = output.getvalue()
        dbody = {'class' if version == 2 else 'set': vectors}
        ibody = {'class': shared} if version == 2 else {}
        q = [{'AddEntity': {'class': shared, '_ref': 1, 'properties': {'sparql_shared': prefix + 'entity'}}},
             {'AddImage': {**ibody, '_ref': 2, 'format': 'png',
                           'properties': {'sparql_shared': prefix + 'image'}}},
             {'AddDescriptor': {**dbody, '_ref': 3,
                                'properties': {'sparql_vector_name': 'near'}}},
             {'AddDescriptor': {**dbody, '_ref': 4,
                                'properties': {'sparql_vector_name': 'far'}}},
             {'AddConnection': {'class': edge, 'src': 1, 'dst': 2}},
             {'AddConnection': {'class': vedge, 'src': 1, 'dst': 3}}]
        okay(db.query(q, [image, np.array([1., 0.], dtype=np.float32).tobytes(
        ), np.array([0., 1.], dtype=np.float32).tobytes()])[0])
        sparql = SPARQL(db, debug=True)
        object_type = 'entity:' + shared if version == 2 else shared
        entity_rows = list(sparql.query(
            f'SELECT ?e ?value WHERE {{ ?e a <http://aperturedb.io/type/{quote(object_type, safe="")}>; p:sparql_shared ?value . }}'))
        assert len(entity_rows) == 1, entity_rows
        assert str(entity_rows[0][1]) == prefix + 'entity', entity_rows
        rows = list(sparql.query(f'SELECT ?e ?i WHERE {{ ?e c:{edge} ?i . }}'))
        assert len(rows) == 1, rows
        uri = rows[0][1]
        assert sparql.get_blob(uri) == image
        assert sparql.get_blob(str(uri)) == image
        assert sparql.get_blobs([uri, uri]) == [image, image]
        arr = sparql.get_image(uri)
        assert arr.shape == (3, 2, 3), arr.shape
        assert np.all(arr == np.array([11, 22, 33])), arr
        type_name = sparql._deduce_type(uri)
        if version == 2:
            assert sparql._typed_categories[type_name] == (
                'image', shared), sparql._typed_categories
            assert sparql._make_safe_prefix(
                'image:' + shared) != sparql._make_safe_prefix('entity:' + shared)
            assert sparql._make_safe_prefix(
                'image:A-B') != sparql._make_safe_prefix('image:A_2d_B')
        knn = f'''SELECT ?d ?name ?distance WHERE {{
          ?d knn:similarTo [knn:set "{vectors}"; knn:k_neighbors 2; knn:engine "Flat"; knn:metric "L2";
                           knn:vector "[1.0,0.0]"; knn:distance ?distance]; p:sparql_vector_name ?name .
        }}'''
        matches = list(sparql.query(knn))
        assert len(matches) == 2, matches
        assert str(matches[0][1]) == 'near', matches
        assert float(matches[0][2]) == 0, matches
        results.append({'transport': cls.__name__, 'port': port,
                       'result': 'PASS', 'prefix': prefix})
        print(json.dumps(results[-1]), flush=True)
    finally:
        try:
            db.query(
                [{'DeleteImage': {'constraints': ['$sparql_shared', '==', prefix + 'image']}}])
            db.query([{'DeleteEntity': {'with_class': shared}}])
            if not utils.remove_descriptorset(vectors):
                print('CLEANUP_PENDING', vectors, flush=True)
            if version == 2:
                db.query([{'RemoveClass': {'target': {'src_type': 'entity', 'dst_type': 'image'},
                         'class': edge, 'src_class': shared, 'dst_class': shared}}])
                db.query(
                    [{'RemoveClass': {'target': 'image', 'class': shared}}])
                db.query(
                    [{'RemoveClass': {'target': 'entity', 'class': shared}}])
        finally:
            utils.client.close()
            db.close()


def exercise_images(endpoint):
    cls = ConnectorRest if endpoint.get("rest") else Connector
    port, version = endpoint["port"], endpoint["hello_version"]
    p = 'ImageAudit' + uuid.uuid4().hex[:9]
    ic = p + 'Images'
    dc = p + 'Vectors'
    ec = p + 'Links'
    db = endpoint_client(endpoint)
    u = Utils(db)
    try:
        assert u.create_vector_index(
            dc, 2, metric=['L2'], engines={'flat': {}})
        out = io.BytesIO()
        Image.new('RGB', (2, 3), (10, 20, 30)).save(out, format='PNG')
        data = out.getvalue()
        image = {'class': ic} if version == 2 else {}
        desc = {'class' if version == 2 else 'set': dc}
        q = [{'AddImage': {**image, '_ref': 1, 'format': 'png', 'properties': {'image_audit': p, 'position': 0}}},
             {'AddImage': {**image, '_ref': 2, 'format': 'png',
                           'properties': {'image_audit': p, 'position': 1}}},
             {'AddDescriptor': {**desc, '_ref': 3}
              }, {'AddDescriptor': {**desc, '_ref': 4}},
             {'AddConnection': {'class': ec, 'src': 1, 'dst': 3}}, {'AddConnection': {'class': ec, 'src': 2, 'dst': 4}}]
        okay(db.query(q, [data, data, np.array([1, 0], dtype=np.float32).tobytes(
        ), np.array([0, 1], dtype=np.float32).tobytes()])[0])
        found = db.query([{'FindImage': {'constraints': [
                         '$image_audit', '==', p], 'sort': 'position', 'uniqueids': True}}])[0]
        okay(found)
        rows = found[0]['FindImage']['entities']
        assert len(rows) == 2, found
        images = Images(db, response=[rows[0]])
        assert 'position' in images.get_props_names()
        similar = images.get_similar_images(dc, 1)
        assert similar.images_ids == [r['_uniqueid']
                                      for r in rows], similar.images_ids
        print(json.dumps({'port': port, 'transport': cls.__name__,
              'result': 'PASS', 'prefix': p}), flush=True)
    finally:
        u.remove_descriptorset(dc)
        db.query([{'DeleteImage': {'constraints': ['$image_audit', '==', p]}}])
        if version == 2:
            db.query([{'RemoveClass': {'target': 'image', 'class': ic}}])
        u.client.close()
        db.close()


class HelperCompatibilityTests(unittest.TestCase):
    @unittest.skipUnless(os.getenv("ADB_COMPAT_ENDPOINTS"),
                         "Set ADB_COMPAT_ENDPOINTS to test real server helpers")
    def test_vector_helpers_metadata_schema_and_atomic_removal(self):
        endpoints = json.loads(os.environ["ADB_COMPAT_ENDPOINTS"])
        self.assertTrue(endpoints)
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                exercise_vectors(endpoint)

    @unittest.skipUnless(os.getenv("ADB_COMPAT_ENDPOINTS"),
                         "Set ADB_COMPAT_ENDPOINTS to test real SPARQL helpers")
    def test_sparql_types_images_connections_and_knn(self):
        if importlib.util.find_spec("rdflib") is None:
            self.skipTest("Install the optional rdflib dependency")
        endpoints = json.loads(os.environ["ADB_COMPAT_ENDPOINTS"])
        self.assertTrue(endpoints)
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                exercise_sparql(endpoint)

    @unittest.skipUnless(os.getenv("ADB_COMPAT_ENDPOINTS"),
                         "Set ADB_COMPAT_ENDPOINTS to test real image helpers")
    def test_image_similarity_and_typed_property_names(self):
        endpoints = json.loads(os.environ["ADB_COMPAT_ENDPOINTS"])
        self.assertTrue(endpoints)
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                exercise_images(endpoint)
