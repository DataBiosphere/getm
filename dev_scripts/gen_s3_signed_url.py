#!/usr/bin/env python
import os
import argparse

import boto3

parser = argparse.ArgumentParser()
parser.add_argument("key", type=str)
parser.add_argument("--bucket", type=str, default=os.environ['GETM_S3_TEST_BUCKET'])
parser.add_argument("--expires-in", "-e", type=int, default=3600)
args = parser.parse_args()

s3_args = dict(Bucket=args.bucket, Key=args.key)
url = boto3.client("s3").generate_presigned_url(ClientMethod="get_object",
                                                ExpiresIn=args.expires_in,
                                                Params=s3_args)
print(url)
