# streaming-urls: Streams for cloud storage signed URLs
_streaming-urls_ provides transparently chunked, buffered, io streams for cloud storage signed URLs.

IO opperations are concurrent by default. The number of concurrent threads can be adjusted using the `threads`
parameter, or disabled entirely with `threads=None`.
```
import streaming_urls

# Readable stream:
with streaming_urls.urlopen(url) as fh:
    fh.read(size)

# Process blob in chunks:
for chunk in streaming_urls.iter_content(url):
    my_chunk_processor(chunk)
```

## Installation
```
pip install streaming-urls
```

## Links
Project home page [GitHub](https://github.com/xbrianh/streaming-urls)  
Package distribution [PyPI](https://pypi.org/project/streaming-urls/)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/streaming-urls).

![](https://travis-ci.org/xbrianh/streaming-urls.svg?branch=master) ![](https://badge.fury.io/py/streaming-urls.svg)
