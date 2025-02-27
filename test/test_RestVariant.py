# This test tests variations in the use of the REST API.
# These variations are not recommended practice, but they have been allowed in the past,
# and so customers may be using them.
# The test is not exhaustive, but it does cover the most common variations.

import os
import json
import time
import logging
from types import SimpleNamespace

from aperturedb import ConnectorRest

logger = logging.getLogger(__name__)


class ConnectorRestVariant(ConnectorRest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Test to see if it still works if we drop the terminal slash from the URL
        assert self.url[-1] == '/'
        self.url = self.url[:-1]

    def _query(self, query, blob_array=[], try_resume=True):
        """ Copy of code from ConnectorRest with necessary changes"""
        response_blob_array = []
        # Check the query type
        if not isinstance(query, str):  # assumes json
            query_str = json.dumps(query)
        else:
            query_str = query

        files = [
            ('query', (None, query_str)),
        ]

        for i, blob in enumerate(blob_array):
            # Test to see if it still works if we change the blob key
            files.append((f'blobs_{i}', blob))

        # Set Auth token, only when not authenticated before
        if self.shared_data.session and self.shared_data.session.valid():
            headers = {'Authorization': "Bearer " +
                       self.shared_data.session.session_token}
        else:
            headers = None
        tries = 0
        response = SimpleNamespace()
        response.status_code = 0
        while tries < self.config.retry_max_attempts:
            tries += 1
            try:
                response = self.http_session.post(self.url,
                                                  headers=headers,
                                                  files=files,
                                                  verify=self.use_ssl)
                if response.status_code == 200:
                    # Parse response:
                    json_response = json.loads(response.text)
                    import base64
                    response_blob_array = [base64.b64decode(
                        b) for b in json_response['blobs']]
                    self.last_response = json_response["json"]
                    break
                logger.error(
                    f"Response not OK = {response.status_code} {response.text[:1000]}\n\
                        attempt [{tries}/3] .. PID = {os.getpid()}")
            except Exception as e:
                logger.error(f"Exception during http.post = {e}\n\
                        attempt [{tries}/3] .. PID = {os.getpid()}")

            time.sleep(self.config.retry_interval_seconds)

            self.connect()
            if try_resume:
                self._renew_session()

        if tries == self.config.retry_max_attempts:
            raise Exception(
                f"Could not query ApertureDB {self.config} using REST.")
        return (self.last_response, response_blob_array)
