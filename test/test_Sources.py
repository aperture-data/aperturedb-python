import unittest
from unittest.mock import patch, MagicMock
from aperturedb.Sources import Sources
import botocore.exceptions
from botocore import UNSIGNED
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import Forbidden


class TestSources(unittest.TestCase):
    def setUp(self):
        self.sources = Sources(n_download_retries=1)
        self.validator = lambda x: True

    @patch('boto3.client')
    def test_s3_fallback_nocreds(self, mock_client_factory):
        # Setup mock client
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # Raise NoCredentialsError on first call, succeed on second
        mock_client.get_object.side_effect = [
            botocore.exceptions.NoCredentialsError(),
            {'Body': MagicMock(read=lambda: b'mock_data')}
        ]

        success, img = self.sources.load_from_s3_url(
            "s3://bucket/image.jpg", self.validator)

        self.assertTrue(success)
        # Verify boto3.client was called with UNSIGNED config
        self.assertEqual(mock_client_factory.call_count, 2)
        # The second call should have signature_version=UNSIGNED
        _, kwargs = mock_client_factory.call_args_list[1]
        self.assertEqual(kwargs['config'].signature_version, UNSIGNED)

    @patch('boto3.client')
    def test_s3_fallback_403(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # Raise ClientError 403 on first call, succeed on second
        error_response = {'Error': {'Code': '403'},
                          'ResponseMetadata': {'HTTPStatusCode': 403}}
        mock_client.get_object.side_effect = [
            botocore.exceptions.ClientError(error_response, 'GetObject'),
            {'Body': MagicMock(read=lambda: b'mock_data')}
        ]

        success, img = self.sources.load_from_s3_url(
            "s3://bucket/image.jpg", self.validator)

        self.assertTrue(success)
        self.assertEqual(mock_client_factory.call_count, 2)
        _, kwargs = mock_client_factory.call_args_list[1]
        self.assertEqual(kwargs['config'].signature_version, UNSIGNED)

    @patch('google.cloud.storage.Client')
    def test_gs_fallback_nocreds(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        mock_anon_client = MagicMock()
        mock_client_factory.create_anonymous_client.return_value = mock_anon_client

        # Original client raises exception
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = DefaultCredentialsError(
            "no creds")
        mock_client.bucket.return_value.blob.return_value = mock_blob

        # Anonymous client succeeds
        mock_anon_blob = MagicMock()
        mock_anon_blob.download_as_bytes.return_value = b'mock_data'
        mock_anon_client.bucket.return_value.blob.return_value = mock_anon_blob

        success, img = self.sources.load_from_gs_url(
            "gs://bucket/image.jpg", self.validator)

        self.assertTrue(success)
        mock_client_factory.create_anonymous_client.assert_called_once()

    @patch('google.cloud.storage.Client')
    def test_gs_fallback_403(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        mock_anon_client = MagicMock()
        mock_client_factory.create_anonymous_client.return_value = mock_anon_client

        # Original client raises exception
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = Forbidden("403")
        mock_client.bucket.return_value.blob.return_value = mock_blob

        # Anonymous client succeeds
        mock_anon_blob = MagicMock()
        mock_anon_blob.download_as_bytes.return_value = b'mock_data'
        mock_anon_client.bucket.return_value.blob.return_value = mock_anon_blob

        success, img = self.sources.load_from_gs_url(
            "gs://bucket/image.jpg", self.validator)

        self.assertTrue(success)
        mock_client_factory.create_anonymous_client.assert_called_once()

    @patch('boto3.client')
    def test_s3_non_auth_error_retries(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # Raise generic ClientError on all calls
        error_response = {'Error': {'Code': 'NoSuchKey'},
                          'ResponseMetadata': {'HTTPStatusCode': 404}}
        mock_client.get_object.side_effect = botocore.exceptions.ClientError(
            error_response, 'GetObject')

        success, img = self.sources.load_from_s3_url(
            "s3://bucket/image.jpg", self.validator)

        self.assertFalse(success)
        # Should try initial + retries (1 retry = 2 calls total)
        self.assertEqual(mock_client.get_object.call_count, 2)
        # Should NOT use anonymous fallback
        self.assertEqual(mock_client_factory.call_count, 1)

    @patch('google.cloud.storage.Client')
    def test_gs_non_auth_error_retries(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        # Raise generic Exception on all calls
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = Exception("Some other error")
        mock_client.bucket.return_value.blob.return_value = mock_blob

        success, img = self.sources.load_from_gs_url(
            "gs://bucket/image.jpg", self.validator)

        self.assertFalse(success)
        self.assertEqual(mock_blob.download_as_bytes.call_count, 2)
        mock_client_factory.create_anonymous_client.assert_not_called()


class TestSourcesCaching(unittest.TestCase):
    @patch('boto3.client')
    def test_s3_client_caching(self, mock_client_factory):
        sources = Sources(n_download_retries=1)
        def validator(x): return True

        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        mock_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: b'mock_data')}

        sources.load_from_s3_url("s3://bucket/image1.jpg", validator)
        sources.load_from_s3_url("s3://bucket/image2.jpg", validator)

        # client should only be created once
        self.assertEqual(mock_client_factory.call_count, 1)

    @patch('google.cloud.storage.Client')
    def test_gs_client_caching(self, mock_client_factory):
        sources = Sources(n_download_retries=1)
        def validator(x): return True

        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b'mock_data'
        mock_client.bucket.return_value.blob.return_value = mock_blob

        sources.load_from_gs_url("gs://bucket/image1.jpg", validator)
        sources.load_from_gs_url("gs://bucket/image2.jpg", validator)

        self.assertEqual(mock_client_factory.call_count, 1)


if __name__ == '__main__':
    unittest.main()
