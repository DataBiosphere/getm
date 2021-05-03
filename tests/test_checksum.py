#!/usr/bin/env python
import io
import os
import sys
import requests
import unittest
import warnings
from uuid import uuid4
from random import randint

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from streaming_urls.reader import http
from streaming_urls.checksum import MB, S3Etag, S3MultiEtag, GSCRC32C, _s3_multipart_layouts, part_count_from_s3_etag
from tests.infra import GS, S3, suppress_warnings


def setUpModule():
    suppress_warnings()
    GS.setup()
    S3.setup()

def tearDownModule():
    GS.client._http.close()

class TestChecksum(unittest.TestCase):
    def test_s3_etag(self):
        expected = "85cb78a5c58c243195d5f5fb84027968-4"
        bucket, key = "1000genomes", "analysis.sequence.index"
        url = generate_presigned_GET_url(bucket, key)
        number_of_parts = part_count_from_s3_etag(http.checksums(url)['s3_etag'])
        data = boto3.client("s3").get_object(Bucket=bucket, Key=key)['Body'].read()
        s3etags = S3MultiEtag(http.size(url), number_of_parts)
        s3etags.update(data)
        self.assertTrue(s3etags.matches(expected))

    def test_gs_crc32c(self):
        key = f"test_read/{uuid4()}"
        expected_data = os.urandom(1024)
        GS.bucket.blob(key).upload_from_file(io.BytesIO(expected_data))
        gs_url = GS.generate_presigned_GET_url(key)
        expected = http.checksums(gs_url)['gs_crc32c']
        crc32c = GSCRC32C()
        data = GS.bucket.blob(key).download_as_bytes()
        crc32c.update(data)
        self.assertTrue(crc32c.matches(expected))

    def test_s3_multipart_layouts(self):
        size = 54743580
        num_parts = 4
        expected = [14680064, 15728640, 16777216, 17825792]
        out = _s3_multipart_layouts(size, num_parts)
        self.assertEqual(expected, out)

        size = 4 * MB
        num_parts = 4
        expected = [MB]
        out = _s3_multipart_layouts(size, num_parts)
        self.assertEqual(expected, out)

        size = 5 * MB
        num_parts = 5
        expected = [MB]
        out = _s3_multipart_layouts(size, num_parts)
        self.assertEqual(expected, out)

def generate_presigned_GET_url(bucket: str, key: str, response_disposition: str=None) -> str:
    import boto3
    client = boto3.client("s3")
    args = dict(Bucket=bucket, Key=key)
    if response_disposition is not None:
        args['ResponseContentDisposition'] = response_disposition
    url = boto3.client("s3").generate_presigned_url(ClientMethod="get_object", Params=args)
    return url

if __name__ == '__main__':
    unittest.main()
