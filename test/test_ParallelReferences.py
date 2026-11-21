"""Query batching must keep ownership and graph references within each input."""
import copy
from io import BytesIO
import json
import os
from uuid import uuid4

from PIL import Image
import pytest

from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb.ParallelQuery import ParallelQuery


@pytest.mark.parametrize('field', ['image_ref', 'video_ref', 'bounding_box_ref', 'polygon_ref',
                                   'frame_ref', 'clip_ref', 'roi_1', 'roi_2'])
def test_owner_references_are_remapped_for_each_batched_query(field):
    query = [{'FindImage': {'_ref': 10}}, {'FindBoundingBox': {'_ref': 20}},
             {'FindImage': {field: 20}}]
    commands, _ = ParallelQuery.__new__(ParallelQuery).generate_batch([
        (copy.deepcopy(query), []), (copy.deepcopy(query), [])])
    assert commands[2]['FindImage'][field] == 2
    assert commands[5]['FindImage'][field] == 5


def test_connect_arrays_and_nested_relationships_are_remapped():
    query = [{'FindEntity': {'_ref': 'parent'}},
             {'FindConnection': {'_ref': 'edge'}},
             {'AddEntity': {'connect': [{'ref': 'parent', 'class': 'Rel'}]}},
             {'FindEntity': {'is_connected_to': {'any': [{'all': [{'ref': 'parent'}]}]},
                             'is_endpoint_of': {'ref': 'edge'}}}]
    commands, _ = ParallelQuery.__new__(ParallelQuery).generate_batch([
        (copy.deepcopy(query), []), (copy.deepcopy(query), [])])
    assert commands[6]['AddEntity']['connect'][0]['ref'] == 5
    assert commands[7]['FindEntity']['is_connected_to']['any'][0]['all'][0]['ref'] == 5
    assert commands[7]['FindEntity']['is_endpoint_of']['ref'] == 6


@pytest.mark.skipif(not os.getenv('ADB_COMPAT_ENDPOINTS'), reason='Set ADB_COMPAT_ENDPOINTS for native live tests')
def test_native_parallel_owner_references():
    endpoints = [entry for entry in json.loads(
        os.environ['ADB_COMPAT_ENDPOINTS']) if entry['hello_version'] == 2]
    if not endpoints:
        pytest.skip('No native hello-v2 endpoint configured')
    for endpoint in endpoints:
        cls = ConnectorRest if endpoint.get('rest') else Connector
        client = cls(host=endpoint.get('host', '127.0.0.1'), port=endpoint['port'],
                     user=os.getenv('ADB_COMPAT_USERNAME', 'admin'),
                     password=os.getenv('ADB_COMPAT_PASSWORD', 'admin'),
                     use_ssl=endpoint.get('use_ssl', False), ca_cert=endpoint.get('ca_cert'),
                     verify_hostname=endpoint.get('verify_hostname', False))
        name = 'BatchRefs' + uuid4().hex
        try:
            data = []
            for identity, color in [('first', 'red'), ('second', 'blue')]:
                image = BytesIO()
                Image.new('RGB', (2, 2), color=color).save(image, format='PNG')
                query = [{'AddImage': {'class': name, '_ref': 10, 'properties': {'identity': identity}}},
                         {'AddBoundingBox': {'class': name, '_ref': 20, 'image_ref': 10,
                                             'rectangle': {'x': 0, 'y': 0, 'width': 1, 'height': 1}}},
                         {'FindImage': {'bounding_box_ref': 20,
                                        'results': {'list': ['identity']}}},
                         {'AddEntity': {'class': name, 'connect': [
                             {'ref': 10, 'class': 'SourceImage'}, {'ref': 20, 'class': 'SourceBox'}]}}]
                data.append((query, [image.getvalue()]))
            commands, blobs = ParallelQuery.__new__(
                ParallelQuery).generate_batch(data)
            response, _ = client.query(commands, blobs)
            assert all(next(iter(item.values()))[
                       'status'] == 0 for item in response), response
            assert response[2]['FindImage']['entities'][0]['identity'] == 'first'
            assert response[6]['FindImage']['entities'][0]['identity'] == 'second'
        finally:
            client.query([{'DeleteEntity': {'with_class': name}}, {
                         'DeleteImage': {'with_class': name}}])
            client.close()
