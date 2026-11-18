"""Optional live SDK compatibility matrix, with no cloud dependencies.

Set ADB_COMPAT_ENDPOINTS to a JSON array of endpoint objects, for example:
[{"port":55020,"hello_version":1}, {"port":55010,"hello_version":2},
 {"port":55220,"hello_version":1,"rest":true},
 {"port":55110,"hello_version":2,"rest":true}].

Endpoints may also specify host, use_ssl, ca_cert, and verify_hostname. The
database username/password default to admin/admin and can be supplied through
ADB_COMPAT_USERNAME and ADB_COMPAT_PASSWORD. Use isolated test databases: each
case inserts uniquely identified blobs and deliberately abandons a read.
"""

import json
import os
import unittest
import uuid

from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest


class ProtocolCompatibilityTests(unittest.TestCase):
    @unittest.skipUnless(os.getenv('ADB_COMPAT_ENDPOINTS'),
                         'Set ADB_COMPAT_ENDPOINTS to test real 0.20 and 1.0 servers')
    def test_native_tls_http_and_streaming_matrix(self):
        endpoints = json.loads(os.environ['ADB_COMPAT_ENDPOINTS'])
        self.assertTrue(endpoints)
        for endpoint in endpoints:
            with self.subTest(port=endpoint['port'], rest=endpoint.get('rest', False),
                              version=endpoint['hello_version']):
                self.exercise_endpoint(endpoint)

    def exercise_endpoint(self, endpoint):
        cls = ConnectorRest if endpoint.get('rest') else Connector
        client = cls(
            host=endpoint.get('host', '127.0.0.1'), port=endpoint['port'],
            user=os.getenv('ADB_COMPAT_USERNAME', 'admin'),
            password=os.getenv('ADB_COMPAT_PASSWORD', 'admin'),
            use_ssl=endpoint.get('use_ssl', False),
            ca_cert=endpoint.get('ca_cert'),
            verify_hostname=endpoint.get('verify_hostname', False))
        try:
            version = endpoint['hello_version']
            self.assertEqual(client.hello_version, version)
            for payload in (b'raw\x00\r\n\xff' * (16 * 1024), b''):
                identifier = uuid.uuid4().hex
                add = {'properties': {'transport_run': identifier}}
                find = {'blobs': True}
                # Raw query() preserves caller syntax. Helpers use the same
                # hello selection for version-specific command generation.
                if version == 2:
                    add['class'] = 'TransportCompatibility' + identifier
                    find.update(with_class=add['class'],
                                constraints=['$transport_run', '==', identifier])
                else:
                    find['constraints'] = {'transport_run': ['==', identifier]}
                response, blobs = client.query(
                    [{'AddBlob': add}, {'FindBlob': find}], [payload])
                self.assert_success(response)
                self.assertEqual(blobs, [payload])
                with client.query_stream([{'FindBlob': find}]) as response:
                    self.assert_success(response.json)
                    self.assertEqual([b''.join(blob.iter_chunks(8192))
                                      for blob in response.blobs], [payload])
                    self.assertTrue(response.complete)
                with client.query_stream([{'FindBlob': find}]) as response:
                    if payload:
                        self.assertEqual(
                            next(response.blobs).read(1), payload[:1])
                    # Leave either unread bytes or an unread blob header.
                self.assertFalse(client.connected)
                response, blobs = client.query([{'GetStatus': {}}])
                self.assert_success(response)
                self.assertFalse(blobs)
                self.assertEqual(client.hello_version, version)
        finally:
            client.close()

    def assert_success(self, response):
        self.assertIsInstance(response, list, response)
        for command in response:
            self.assertEqual(next(iter(command.values()))
                             ['status'], 0, response)


if __name__ == '__main__':
    unittest.main()
