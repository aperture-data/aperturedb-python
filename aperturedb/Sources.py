import time
import requests
import logging

logger = logging.getLogger(__name__)


class Sources():
    """
    **Load data from various resources**
    """

    def __init__(self, n_download_retries, **kwargs):

        self.n_download_retries = n_download_retries

        # Use custom clients if specified
        self.s3 = None if "s3_client" not in kwargs else kwargs["s3_client"]
        self.http_client = requests.Session(
        ) if "http_client" not in kwargs else kwargs["http_client"]

    def load_from_file(self, filename):
        """
        Load data from a file.
        """
        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.error(f"VALIDATION ERROR: {filename}")
            logger.exception(e)
        finally:
            if not fd.closed:
                fd.close()
        return False, None

    def load_from_http_url(self, url, validator):
        """
        Load data from a http url.
        """
        import numpy as np

        retries = 0
        while True:
            imgdata = self.http_client.get(url)
            if imgdata.ok and ("Content-Length" not in imgdata.headers or int(imgdata.headers["Content-Length"]) == imgdata.raw._fp_bytes_read):
                imgbuffer = np.frombuffer(imgdata.content, dtype='uint8')
                if not validator(imgbuffer):
                    logger.error(f"VALIDATION ERROR: {url}")
                    return False, None

                return imgdata.ok, imgdata.content
            else:
                if retries >= self.n_download_retries:
                    break
                logger.warning(f"Retrying object: {url}")
                retries += 1
                time.sleep(2)

        return False, None

    def load_from_s3_url(self, s3_url, validator):
        import numpy as np
        import boto3
        import botocore.exceptions

        retries = 0
        tried_anonymous = False
        active_s3_client = self.s3

        while True:
            try:
                bucket_name = s3_url.split("/")[2]
                object_name = s3_url.split("s3://" + bucket_name + "/")[-1]

                if active_s3_client is None:
                    active_s3_client = boto3.client('s3')

                s3_response_object = active_s3_client.get_object(
                    Bucket=bucket_name, Key=object_name)
                img = s3_response_object['Body'].read()
                imgbuffer = np.frombuffer(img, dtype='uint8')
                if not validator(imgbuffer):
                    logger.error(f"VALIDATION ERROR: {s3_url}")
                    return False, None

                return True, img
            except Exception as e:
                is_auth_error = False
                if isinstance(e, botocore.exceptions.NoCredentialsError):
                    is_auth_error = True
                elif isinstance(e, botocore.exceptions.ClientError):
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code in ['401', '403', 'AccessDenied']:
                        is_auth_error = True
                elif "'NoneType' object has no attribute 'get_object'" in str(e):
                    is_auth_error = True

                if not tried_anonymous and is_auth_error:
                    from botocore import UNSIGNED
                    from botocore.config import Config
                    active_s3_client = boto3.client(
                        's3', config=Config(signature_version=UNSIGNED))
                    tried_anonymous = True
                    continue

                if retries >= self.n_download_retries:
                    break
                logger.warning(f"Retrying object: {s3_url}", exc_info=True)
                retries += 1
                time.sleep(2)

        logger.error(f"S3 ERROR: {s3_url}")
        return False, None

    def load_from_gs_url(self, gs_url, validator):
        import numpy as np
        from google.cloud import storage

        retries = 0
        tried_anonymous = False
        client = None
        while True:
            try:
                if client is None:
                    client = storage.Client()

                bucket_name = gs_url.split("/")[2]
                object_name = gs_url.split("gs://" + bucket_name + "/")[-1]

                blob = client.bucket(bucket_name).blob(
                    object_name).download_as_bytes()
                imgbuffer = np.frombuffer(blob, dtype='uint8')
                if not validator(imgbuffer):
                    logger.warning(f"VALIDATION ERROR: {gs_url}")
                    return False, None
                return True, blob
            except Exception as e:
                from google.auth.exceptions import DefaultCredentialsError
                from google.api_core.exceptions import Forbidden, Unauthorized
                
                is_auth_error = isinstance(e, (DefaultCredentialsError, Forbidden, Unauthorized))

                if not tried_anonymous and is_auth_error:
                    client = storage.Client.create_anonymous_client()
                    tried_anonymous = True
                    continue

                if retries >= self.n_download_retries:
                    break
                logger.warning(f"Retrying object: {gs_url}", exc_info=True)
                retries += 1
                time.sleep(2)

        logger.error(f"GS ERROR: {gs_url}")
        return False, None
