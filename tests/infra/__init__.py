import io
import os
import logging
import warnings
import datetime
import contextlib
from functools import wraps
from typing import Tuple

import boto3
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket


def get_env(name: str) -> str:
    val = os.environ.get(name)
    if val is None:
        raise RuntimeError(f"Please set the '{name}' environment variable to run tests.")
    return val

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
        if os.environ.get("GETM_GOOGLE_APPLICATION_CREDENTIALS"):
            cls.client = Client.from_service_account_json(os.environ['GETM_GOOGLE_APPLICATION_CREDENTIALS'])
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            cls.client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        else:
            cls.client = Client()
        cls.bucket = cls.client.bucket(get_env('GETM_GS_TEST_BUCKET'))

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
        cls.bucket = boto3.resource("s3").Bucket(get_env('GETM_S3_TEST_BUCKET'))

def suppress_warnings():
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    # Suppress unclosed socket warnings
    warnings.simplefilter("ignore", ResourceWarning)

def suppress_output(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        logging.disable(logging.CRITICAL)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                with contextlib.redirect_stderr(io.StringIO()):
                    return func(*args, **kwargs)
        finally:
            logging.disable(logging.NOTSET)
    return wrapped
