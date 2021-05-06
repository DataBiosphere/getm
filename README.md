# getm: Fast reads with integrity for data URLs
_getm_ provides transparently chunked, buffered, io streams for cloud storage signed URLs.

IO opperations are concurrent by default. The number of concurrent threads can be adjusted using the `threads`
parameter, or disabled entirely with `threads=None`.
```
import getm

# Readable stream:
with getm.urlopen(url) as fh:
    fh.read(size)

# Process blob in chunks:
for chunk in getm.iter_content(url):
    my_chunk_processor(chunk)
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
