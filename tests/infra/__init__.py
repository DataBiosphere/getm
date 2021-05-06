import io
import os
import warnings
import datetime
from typing import Tuple

import boto3
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket


class GS:
    client = None
    bucket = None

    @classmethod
    def generate_presigned_GET_url(cls, key: str, response_disposition: str=None) -> str:
        kwargs = dict()
        if response_disposition is not None:
            kwargs['response_disposition'] = response_disposition
        return cls.bucket.blob(key).generate_signed_url(datetime.timedelta(days=1),
                                                        version="v4",
                                                        **kwargs)

    @classmethod
    def setup(cls):
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            cls.client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        elif os.environ.get("GETM_TEST_CREDENTIALS"):
            import json
            import base64
            from google.oauth2.service_account import Credentials
            creds_info = json.loads(base64.b64decode(os.environ.get("GETM_TEST_CREDENTIALS")))
            creds = Credentials.from_service_account_info(creds_info)
            cls.client = Client(credentials=creds)
        else:
            cls.client = Client()
        cls.bucket = cls.client.bucket("gs-chunked-io-test")

    @classmethod
    def put_fixture(cls, sz=1024 * 1024 * 1024) -> Tuple[str, int]:
        key = "getm-large-file"
        blob = cls.bucket.get_blob(key)
        if not blob:
            data = os.urandom(sz)
            cls.bucket.blob(key).upload_from_file(io.BytesIO(data))
        return key, sz

class S3:
    bucket = None

    @classmethod
    def generate_presigned_GET_url(cls, key: str, response_disposition: str=None) -> str:
        args = dict(Bucket=cls.bucket.name, Key=key)
        if response_disposition is not None:
            args['ResponseContentDisposition'] = response_disposition
        url = boto3.client("s3").generate_presigned_url(ClientMethod="get_object", Params=args)
        return url

    @classmethod
    def setup(cls):
        cls.bucket = boto3.resource("s3").Bucket("org-hpp-ssds-staging-test")

def suppress_warnings():
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    # Suppress unclosed socket warnings
    warnings.simplefilter("ignore", ResourceWarning)
