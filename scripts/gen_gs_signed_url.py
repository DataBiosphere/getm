#!/usr/bin/env python
"""
Given a Google Storage cloud native url, generate a signed URL.

The environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is expected to point to a service account credentials json
file.
"""
import os
import sys
import datetime

from google.cloud.storage import Client


gs_native_url = sys.argv[1]
assert gs_native_url.startswith("gs://")
bucket_name, key = gs_native_url[5:].split("/", 1)
client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
blob = client.bucket(bucket_name).get_blob(key)
assert blob is not None
url = blob.generate_signed_url(datetime.timedelta(days=1), version="v4")
print(url)
