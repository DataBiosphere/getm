#!/usr/bin/env python
"""Given a Google Storage cloud native url, generate a signed URL.

The environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is expected to point to a service account credentials json
file.
"""
import os
import sys
import argparse
import datetime

from google.cloud.storage import Client


parser = argparse.ArgumentParser()
parser.add_argument("key", type=str)
parser.add_argument("--bucket", type=str, default=os.environ['GETM_GS_TEST_BUCKET'])
parser.add_argument("--expires-in", "--expires", "-e", type=int, default=3600)
args = parser.parse_args()

client = Client.from_service_account_json(os.environ['GETM_GOOGLE_APPLICATION_CREDENTIALS'])
blob = client.bucket(args.bucket).get_blob(args.key)
assert blob is not None
url = blob.generate_signed_url(datetime.timedelta(seconds=args.expires_in), version="v4")
print(url)
