"""Opt-in real fork regression for dataset connections and authentication locks."""
import json
import os
import select
import signal
import sys
from types import SimpleNamespace
import unittest
from uuid import uuid4

from aperturedb.Connector import Connector
from aperturedb.TensorFlowDataset import ApertureDBTensorFlowDataset


@unittest.skipUnless(hasattr(os, 'fork') and os.environ.get('SDK_LIVE_NATIVE_PORTS'),
                     'Set SDK_LIVE_NATIVE_PORTS for disposable Linux test databases')
class DatasetForkCompatibility(unittest.TestCase):
    def _child(self, dataset, operation):
        inherited = dataset.client.shared_data
        # A lock held in another thread/process at fork remains locked in the
        # child. A new socket sharing this state would still deadlock on login.
        inherited.lock.acquire()
        read_fd, write_fd = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(read_fd)
            result = {}
            code = 0
            try:
                if operation == 'batch':
                    dataset.get_batch(0)
                    result['blob'] = dataset.batch_blobs[0].decode()
                    result['label'] = dataset.batch_labels[0]
                else:
                    # Only the optional framework's constructors are stubbed;
                    # label inference runs the real SDK query against the DB.
                    sys.modules['tensorflow'] = SimpleNamespace(
                        int32='int32', float32='float32', string='string', uint8='uint8',
                        TensorSpec=lambda **kwargs: kwargs,
                        data=SimpleNamespace(Dataset=SimpleNamespace(
                            from_generator=lambda *args, **kwargs: kwargs)))
                    dataset.get_dataset()
                    result['label_type'] = dataset.label_type
                result['fresh_session'] = dataset.client.shared_data is not inherited
                result['fresh_lock'] = dataset.client.shared_data.lock is not inherited.lock
                result['ok'] = True
            except BaseException as error:
                result = {'ok': False, 'error': repr(error)}
                code = 1
            os.write(write_fd, json.dumps(result).encode())
            os.close(write_fd)
            os._exit(code)
        os.close(write_fd)
        try:
            readable, _, _ = select.select([read_fd], [], [], 5)
            if not readable:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                self.fail(
                    'Forked dataset reused the parent authentication lock or connection')
            payload = os.read(read_fd, 8192)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0, payload)
            result = json.loads(payload)
            self.assertTrue(result.get('ok'), result)
            self.assertTrue(result['fresh_session'], result)
            self.assertTrue(result['fresh_lock'], result)
            if operation == 'batch':
                self.assertEqual(result['blob'], 'fork-payload')
                self.assertEqual(result['label'], 7)
            else:
                self.assertEqual(result['label_type'], 'int32')
        finally:
            os.close(read_fd)
            inherited.lock.release()

    def test_forked_batches_and_label_inference_use_fresh_authentication(self):
        for port in os.environ['SDK_LIVE_NATIVE_PORTS'].split(','):
            for operation in ['batch', 'label']:
                with self.subTest(port=port, operation=operation):
                    client = Connector(host='127.0.0.1', port=int(port), user='admin',
                                       password='admin', use_ssl=False)
                    marker = 'SDKFork' + uuid4().hex
                    modern = client.hello_version >= 2
                    scope = {'with_class': marker} if modern else {}
                    try:
                        response, _ = client.query([{'AddBlob': {
                            **({'class': marker} if modern else {}),
                            'properties': {'fork_fixture': marker, 'label': 7}}}], [b'fork-payload'])
                        self.assertEqual(response[0]['AddBlob']['status'], 0)
                        dataset = ApertureDBTensorFlowDataset(client, [{'FindBlob': {
                            **scope, 'constraints': ['$fork_fixture', '==', marker]}}],
                            batch_size=1, label_prop='label')
                        self.assertEqual(dataset.total_elements, 1)
                        self._child(dataset, operation)
                        # The child's independent login/query leaves the parent
                        # transport and session usable after its lock is released.
                        response, _ = client.query([{'FindBlob': {
                            **scope, 'constraints': ['$fork_fixture', '==', marker],
                            'results': {'count': True}}}])
                        self.assertEqual(response[0]['FindBlob']['count'], 1)
                    finally:
                        client.query([{'DeleteBlob': {
                            **scope, 'constraints': ['$fork_fixture', '==', marker]}}])
                        client.close()


if __name__ == '__main__':
    unittest.main()
