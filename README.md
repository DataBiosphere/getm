# getm: Fast reads with integrity for data URLs
_getm_ provides fast binary reads for HTTP URLs using
[multiprocessing](https://docs.python.org/3/library/multiprocessing.html) and
[shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html).

Data is downloaded in background processes and made availabe as references to shared memory. There are no buffer
copies, but memory references must be released by the caller, which makes working with getm a bit different than
typical Python IO streams. But still easy, and fast. In the case of part iteration, memoryview objects are released
for you.

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
	# Note that 'part.release()' is not needed in an iterator context
```

CLI
```
getm https://my-cool-url my-local-file
```

## Testing

During tests, signed URLs are generated that point to data in S3 and GS buckets. Data is repopulated during each test.
You must have credentials available to read and write to the test buckets, and to generate signed URLs.

Set the following environment variables to the GS and S3 test bucket names, respectively:
- `GETM_GS_TEST_BUCKET`
- `GETM_S3_TEST_BUCKET`

### GCP Credentials

Generating signed URLs during tests requires service account credentials, which are made available to the test suite by
setting the environment variable
```
export GETM_GOOGLE_APPLICATION_CREDENTIALS=my-creds.json
```

### AWS Credentials

Follow these [instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for configuring
the AWS CLI.

## Installation
```
pip install getm
```

### Shared Memory Size Tests
Before release, tests should be performed on systems with various amounts of shared memory. Good choices are 64M and
8G. It is also highly encouraged for development work on the shared memory algorithms and configurations of
`getm`.

Shared memory can be resized on Ubuntu systems, and likely other Linux systems, with the bundled convenience script
`dev_scripts/resize_shm.sh`. Either sudo or root access is required.:
```
sudo dev_scripts/resize_shm.sh 64M
sudo dev_scripts/resize_shm.sh 8G
```

## [sharedmemory](https://docs.python.org/3/library/multiprocessing.shared_memory.html) backport to Python 3.7

getm relies on the `sharedmemory` module, which was introduced in Python 3.8. Since a large portion of getm's audience
relies on Python 3.7, a C extension
[backport](https://github.com/DataBiosphere/getm/tree/master/getm/concurrent/shared_memory_37) of `sharedmemory` is included.

The backport adds significant complexity getm's code base, requiring C/C++ knowlege to maintain, as well as knowledge of
[CPython](https://github.com/python/cpython). It will be removed when enough getm users have migrated to Python 3.8 or
greater.

## Links
Project home page [GitHub](https://github.com/DataBiosphere/getm)  
Package distribution [PyPI](https://pypi.org/project/getm/)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/getm).

![](https://travis-ci.org/DataBiosphere/getm.svg?branch=master) ![](https://badge.fury.io/py/getm.svg)

## Credits
getm was created by [Brian Hannafious](https://github.com/xbrianh) at the
[UCSC Genomics Institute](https://ucscgenomics.soe.ucsc.edu/).

Special thanks to [Michael Baumann](https://github.com/mbaumann-broad) and
[Lon Blauvelt](https://github.com/DailyDreaming) for critical input and testing.
