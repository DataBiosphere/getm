# getm: Fast reads with integrity for data URLs
_getm_ provides fast binary reads for HTTP URLs using
[multiprocessing](https://docs.python.org/3/library/multiprocessing.html) and
[shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html).

Data is downloaded in background processes and made availabe as references to shared memory. There are no buffer
copies, but memory references must be released by the caller, which makes working with getm a bit different than
typical Python IO streams. But still easy, and fast.

Python API methods accept a parameter, `concurrency`, which controls the mode of operation of mget:
1. Default `concurrency == 1`: Download data in a single background process, using a single HTTP request that is kept
   alive during the course of the download.
1. `concurrency > 1`:  Up to `concurrency` HTTP range requests will be made concurrently, each in a separate background
   process.
1. `concurrency == None`: Data is read on the main process. In this mode, getm is a wrapper for
   [requests](https://docs.python-requests.org/en/master/).

Python API
```
import getm

# Readable stream:
with getm.urlopen(url) as fh:
    data = fh.read(size)
	data.release()

# Process data in parts:
for part in getm.iter_content(url, chunk_size=1024 * 1024):
    my_chunk_processor(part)
	part.release()
```

CLI
```
getm https://my-cool-url my-local-file
```

## Testing

During tests, signed URLs are generated that point to data in S3 and GS buckets. The data is repopulated
during each test.

### Credentials

To run tests you must be properly credentialed. For S3 you must have access to the test bucket, and sufficient
privliages to upload data and generate signed URLs. For GS you must have service account credentials privlidged with
similar access to the GS bucket. The service account are made available by setting the environment variable
```
export GOOGLE_APPLICATION_CREDENTIALS=my-creds.json
```

## Installation
```
pip install getm
```

## Links
Project home page [GitHub](https://github.com/xbrianh/getm)  
Package distribution [PyPI](https://pypi.org/project/getm/)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/getm).

![](https://travis-ci.org/xbrianh/getm.svg?branch=master) ![](https://badge.fury.io/py/getm.svg)
